//! Service lifecycle management and discovery over a [`seda_bus::Bus`].
//!
//! seda-bus is a transport: named channels, a shared thread pool, bounded
//! queues, the routing-slip engine. It has no notion of a "service".
//! `ServiceBus` adds that: register services, start / stop / pause them, let
//! them find each other, and watch their health. Each service becomes one
//! seda-bus channel keyed by its name, with the service as that channel's
//! consumer.
//!
//! Mirrors the design of `service-bus-java`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use log::{info, warn};
use seda_bus::{Bus, ChannelConfig, Envelope};

use crate::service::{Service, ServiceContext, ServiceStatus};

/// Lifecycle state of the bus itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BusStatus {
    Stopped,
    Starting,
    Running,
    Paused,
    Stopping,
}

/// Lifecycle commands carried on `envelope.headers["command"]`.
pub const CONTROL_COMMANDS: &[&str] =
    &["register", "unregister", "start", "stop", "pause", "unpause"];

type ServiceStatusListener = Box<dyn Fn(&str, ServiceStatus) + Send + Sync>;
type BusStatusListener = Box<dyn Fn(BusStatus) + Send + Sync>;

struct Inner {
    seda: Bus,
    config: Arc<HashMap<String, String>>,
    registered: RwLock<HashMap<String, Arc<dyn Service>>>,
    running: RwLock<HashMap<String, Arc<dyn Service>>>,
    statuses: RwLock<HashMap<String, ServiceStatus>>,
    svc_listeners: Mutex<Vec<ServiceStatusListener>>,
    bus_listeners: Mutex<Vec<BusStatusListener>>,
    status: RwLock<BusStatus>,
    monitor: Mutex<Option<JoinHandle<()>>>,
    monitor_stop: Arc<AtomicBool>,
}

/// A service-management layer over a seda-bus. Cheap to clone (an `Arc` inside).
#[derive(Clone)]
pub struct ServiceBus(Arc<Inner>);

impl ServiceBus {
    /// Create a bus with `workers` shared seda-bus threads (0 = number of cores).
    pub fn new(workers: usize) -> ServiceBus {
        Self::with_config(workers, HashMap::new())
    }

    pub fn with_config(workers: usize, config: HashMap<String, String>) -> ServiceBus {
        ServiceBus(Arc::new(Inner {
            seda: Bus::new(workers),
            config: Arc::new(config),
            registered: RwLock::new(HashMap::new()),
            running: RwLock::new(HashMap::new()),
            statuses: RwLock::new(HashMap::new()),
            svc_listeners: Mutex::new(Vec::new()),
            bus_listeners: Mutex::new(Vec::new()),
            status: RwLock::new(BusStatus::Stopped),
            monitor: Mutex::new(None),
            monitor_stop: Arc::new(AtomicBool::new(false)),
        }))
    }

    /// The underlying seda-bus. Rarely needed.
    pub fn seda_bus(&self) -> &Bus {
        &self.0.seda
    }

    pub fn config(&self) -> &HashMap<String, String> {
        &self.0.config
    }

    // -- lifecycle -----------------------------------------------------

    /// Start the bus and its health monitor.
    pub fn start(&self) {
        self.set_status(BusStatus::Starting);
        self.0.seda.resume();
        self.spawn_monitor();
        self.set_status(BusStatus::Running);
    }

    pub fn pause(&self) -> bool {
        if *self.0.status.read().unwrap() != BusStatus::Running {
            return false;
        }
        for svc in self.0.running.read().unwrap().values() {
            svc.pause();
        }
        self.0.seda.pause();
        self.set_status(BusStatus::Paused);
        true
    }

    pub fn unpause(&self) -> bool {
        if *self.0.status.read().unwrap() != BusStatus::Paused {
            return false;
        }
        self.0.seda.resume();
        for svc in self.0.running.read().unwrap().values() {
            svc.unpause();
        }
        self.set_status(BusStatus::Running);
        true
    }

    pub fn shutdown(&self) -> bool {
        self.do_shutdown(false, Duration::from_secs(5))
    }

    pub fn graceful_shutdown(&self) -> bool {
        self.do_shutdown(true, Duration::from_secs(30))
    }

    fn do_shutdown(&self, graceful: bool, service_timeout: Duration) -> bool {
        self.set_status(BusStatus::Stopping);
        self.0.monitor_stop.store(true, Ordering::Release);
        if let Some(h) = self.0.monitor.lock().unwrap().take() {
            let _ = h.join();
        }

        let names: Vec<String> = self.0.running.read().unwrap().keys().cloned().collect();
        let mut handles = Vec::new();
        for name in names {
            let svc = self.0.running.read().unwrap().get(&name).cloned();
            let Some(svc) = svc else { continue };
            let this = self.clone();
            handles.push(thread::spawn(move || {
                if svc.stop() {
                    this.0.running.write().unwrap().remove(&name);
                }
            }));
        }
        let deadline = Instant::now() + service_timeout;
        for h in handles {
            let _ = h.join();
            if Instant::now() > deadline {
                break;
            }
        }

        let bus_ok = if graceful {
            self.0.seda.shutdown(service_timeout)
        } else {
            self.0.seda.shutdown_now();
            true
        };
        self.set_status(BusStatus::Stopped);
        bus_ok && self.0.running.read().unwrap().is_empty()
    }

    pub fn status(&self) -> BusStatus {
        *self.0.status.read().unwrap()
    }

    // -- registration ------------------------------------------------

    /// Register a service and wire it as its channel's consumer.
    pub fn register(&self, service: Arc<dyn Service>) -> bool {
        let name = service.name().to_string();
        {
            let reg = self.0.registered.read().unwrap();
            if reg.contains_key(&name) {
                return true;
            }
            for dep in service.depends_on() {
                if !reg.contains_key(&dep) {
                    warn!("{name} depends on unregistered {dep:?}; register it first");
                }
            }
        }

        service.set_context(ServiceContext::new(
            self.0.seda.clone(),
            Arc::clone(&self.0.config),
        ));

        self.0.seda.channel(&name, ChannelConfig::default());
        let svc = Arc::clone(&service);
        self.0
            .seda
            .subscribe(&name, move |env: &mut Envelope| svc.handle(env));

        self.0
            .registered
            .write()
            .unwrap()
            .insert(name.clone(), service);
        self.0
            .statuses
            .write()
            .unwrap()
            .insert(name, ServiceStatus::NotInitialized);
        true
    }

    /// Start a registered service. Non-blocking; join with [`Self::await_running`].
    pub fn start_service(&self, name: &str) -> bool {
        let svc = self.0.registered.read().unwrap().get(name).cloned();
        let Some(svc) = svc else {
            warn!("not registered, cannot start: {name}");
            return false;
        };
        if self.0.running.read().unwrap().contains_key(name) {
            return true;
        }
        let this = self.clone();
        let name = name.to_string();
        thread::Builder::new()
            .name(format!("{name}-start"))
            .spawn(move || {
                if svc.start() {
                    this.0.running.write().unwrap().insert(name.clone(), svc);
                    info!("running: {name}");
                } else {
                    warn!("failed to start: {name}");
                }
            })
            .expect("spawn start thread");
        true
    }

    pub fn stop_service(&self, name: &str) -> bool {
        let svc = self.0.running.read().unwrap().get(name).cloned();
        let Some(svc) = svc else { return true };
        let this = self.clone();
        let name = name.to_string();
        thread::spawn(move || {
            if svc.stop() {
                this.0.running.write().unwrap().remove(&name);
            }
        });
        true
    }

    pub fn unregister_service(&self, name: &str) -> bool {
        self.stop_service(name);
        self.0.registered.write().unwrap().remove(name);
        self.0.statuses.write().unwrap().remove(name);
        true
    }

    /// Register then start.
    pub fn register_and_start_service(&self, service: Arc<dyn Service>) -> bool {
        let name = service.name().to_string();
        self.register(service) && self.start_service(&name)
    }

    pub fn register_and_start_services(&self, services: Vec<Arc<dyn Service>>) {
        for svc in services {
            self.register_and_start_service(svc);
        }
    }

    pub fn start_all_registered(&self) {
        let names: Vec<String> = self.0.registered.read().unwrap().keys().cloned().collect();
        for name in names {
            if !self.0.running.read().unwrap().contains_key(&name) {
                self.start_service(&name);
            }
        }
    }

    /// Block until the named services (or all registered) are running, or timeout.
    pub fn await_running(&self, timeout: Duration, names: &[&str]) -> bool {
        let targets: Vec<String> = if names.is_empty() {
            self.0.registered.read().unwrap().keys().cloned().collect()
        } else {
            names.iter().map(|s| s.to_string()).collect()
        };
        let deadline = Instant::now() + timeout;
        loop {
            let running = self.0.running.read().unwrap();
            if targets.iter().all(|n| running.contains_key(n)) {
                return true;
            }
            drop(running);
            if Instant::now() >= deadline {
                return self
                    .0
                    .running
                    .read()
                    .unwrap()
                    .keys()
                    .filter(|k| targets.contains(k))
                    .count()
                    == targets.len();
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    // -- discovery -------------------------------------------------

    pub fn registered_service_names(&self) -> Vec<String> {
        self.0.registered.read().unwrap().keys().cloned().collect()
    }

    pub fn running_service_names(&self) -> Vec<String> {
        self.0.running.read().unwrap().keys().cloned().collect()
    }

    pub fn get_service(&self, name: &str) -> Option<Arc<dyn Service>> {
        self.0.registered.read().unwrap().get(name).cloned()
    }

    /// Running services matching a predicate - e.g.
    /// `bus.find_running_services(|s| s.as_any().is::<I2pService>())`.
    pub fn find_running_services<F>(&self, predicate: F) -> Vec<Arc<dyn Service>>
    where
        F: Fn(&dyn Service) -> bool,
    {
        self.0
            .running
            .read()
            .unwrap()
            .values()
            .filter(|s| predicate(s.as_ref()))
            .cloned()
            .collect()
    }

    pub fn running_services(&self) -> Vec<Arc<dyn Service>> {
        self.0.running.read().unwrap().values().cloned().collect()
    }

    pub fn is_registered(&self, name: &str) -> bool {
        self.0.registered.read().unwrap().contains_key(name)
    }

    pub fn is_running(&self, name: &str) -> bool {
        self.0.running.read().unwrap().contains_key(name)
    }

    /// Live status of a service (the `statuses` map is only a cache the monitor
    /// diffs for listeners).
    pub fn get_service_status(&self, name: &str) -> Option<ServiceStatus> {
        if let Some(s) = self.0.running.read().unwrap().get(name) {
            return Some(s.status());
        }
        if let Some(s) = self.0.registered.read().unwrap().get(name) {
            return Some(s.status());
        }
        self.0.statuses.read().unwrap().get(name).copied()
    }

    pub fn get_service_statuses(&self) -> HashMap<String, ServiceStatus> {
        self.0
            .registered
            .read()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.status()))
            .collect()
    }

    // -- status observation --------------------------------------

    pub fn add_service_status_listener<F>(&self, listener: F)
    where
        F: Fn(&str, ServiceStatus) + Send + Sync + 'static,
    {
        self.0.svc_listeners.lock().unwrap().push(Box::new(listener));
    }

    pub fn add_bus_status_listener<F>(&self, listener: F)
    where
        F: Fn(BusStatus) + Send + Sync + 'static,
    {
        self.0.bus_listeners.lock().unwrap().push(Box::new(listener));
    }

    fn set_status(&self, status: BusStatus) {
        *self.0.status.write().unwrap() = status;
        for l in self.0.bus_listeners.lock().unwrap().iter() {
            l(status);
        }
    }

    /// A monitor thread polls each running service's `status()`, publishes
    /// changes to listeners, and restarts a service that reports `Unstable`.
    fn spawn_monitor(&self) {
        self.0.monitor_stop.store(false, Ordering::Release);
        let this = self.clone();
        let stop = Arc::clone(&self.0.monitor_stop);
        let handle = thread::Builder::new()
            .name("service-bus-monitor".into())
            .spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    thread::sleep(Duration::from_millis(200));
                    let running: Vec<(String, Arc<dyn Service>)> = this
                        .0
                        .running
                        .read()
                        .unwrap()
                        .iter()
                        .map(|(k, v)| (k.clone(), Arc::clone(v)))
                        .collect();
                    for (name, svc) in running {
                        let cur = svc.status();
                        let prev = this.0.statuses.read().unwrap().get(&name).copied();
                        if prev == Some(cur) {
                            continue;
                        }
                        this.0.statuses.write().unwrap().insert(name.clone(), cur);
                        for l in this.0.svc_listeners.lock().unwrap().iter() {
                            l(&name, cur);
                        }
                        if cur == ServiceStatus::Unstable {
                            warn!("{name} UNSTABLE; restarting...");
                            let this2 = this.clone();
                            let name2 = name.clone();
                            thread::spawn(move || {
                                if let Some(svc) =
                                    this2.0.registered.read().unwrap().get(&name2).cloned()
                                {
                                    svc.stop();
                                    if svc.start() {
                                        this2
                                            .0
                                            .running
                                            .write()
                                            .unwrap()
                                            .insert(name2.clone(), svc);
                                    }
                                }
                            });
                        }
                    }
                }
            })
            .expect("spawn monitor");
        *self.0.monitor.lock().unwrap() = Some(handle);
    }

    // -- messaging -----------------------------------------------

    /// Publish an envelope. If `headers["command"]` is a control command
    /// (`headers["service"]` names the target) the bus acts on it first.
    pub fn send(&self, env: Envelope) -> bool {
        self.maybe_command(&env);
        self.0.seda.publish(env, None)
    }

    pub fn send_with_callback<F>(&self, env: Envelope, on_complete: F) -> bool
    where
        F: FnOnce(&Envelope) + Send + 'static,
    {
        self.maybe_command(&env);
        self.0.seda.publish_with_callback(env, None, on_complete)
    }

    fn maybe_command(&self, env: &Envelope) {
        let Some(cmd) = env.headers.get("command") else {
            return;
        };
        if !CONTROL_COMMANDS.contains(&cmd.as_str()) {
            return;
        }
        let Some(name) = env.headers.get("service") else {
            warn!("control command {cmd:?} with no headers[\"service\"]");
            return;
        };
        match cmd.as_str() {
            "start" => {
                self.start_service(name);
            }
            "stop" => {
                self.stop_service(name);
            }
            "unregister" => {
                self.unregister_service(name);
            }
            "pause" => {
                if let Some(s) = self.0.registered.read().unwrap().get(name) {
                    s.pause();
                }
            }
            "unpause" => {
                if let Some(s) = self.0.registered.read().unwrap().get(name) {
                    s.unpause();
                }
            }
            "register" => warn!("'register' over the bus needs a factory; ignored"),
            _ => {}
        }
    }
}
