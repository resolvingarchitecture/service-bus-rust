//! A reusable headless host for a [`ServiceBus`].
//!
//! Rust has no inheritance, so the java `Daemon`'s overridable hooks become
//! closures on a builder:
//!
//! ```no_run
//! use service_bus::{Daemon, ServiceBus};
//! use std::sync::Arc;
//!
//! let handle = Daemon::new()
//!     .config_name("my.config")
//!     .on_bus_started(|bus: &ServiceBus, _cfg| {
//!         // bus.register_and_start_services(vec![Arc::new(FooService::new())]);
//!         bus.await_running(std::time::Duration::from_secs(10), &[]);
//!     })
//!     .on_stopping(|| println!("bye"))
//!     .launch(std::env::args());
//!
//! // install your own signal handler, then:
//! handle.wait();
//! ```
//!
//! The std library has no signal handling, so a real deployment wires
//! SIGINT/SIGTERM (e.g. the `ctrlc` crate) to [`DaemonHandle::shutdown`].
//! Mirrors `ra.servicebus.Daemon` in `service-bus-java`.

use std::collections::HashMap;
use std::fs;
use std::sync::{Arc, Condvar, Mutex};

use log::info;

use crate::bus::ServiceBus;

type Hook1<T> = Box<dyn FnOnce(&T) + Send>;
type Hook2 = Box<dyn FnOnce(&ServiceBus, &HashMap<String, String>) + Send>;
type Hook0 = Box<dyn FnOnce() + Send>;

/// Builder for a [`DaemonHandle`].
#[derive(Default)]
pub struct Daemon {
    config_name: Option<String>,
    workers: usize,
    before_start: Option<Hook1<HashMap<String, String>>>,
    on_bus_started: Option<Hook2>,
    on_stopping: Option<Hook0>,
}

impl Daemon {
    pub fn new() -> Daemon {
        Daemon::default()
    }

    /// Config file name looked up in the working directory.
    pub fn config_name(mut self, name: impl Into<String>) -> Self {
        self.config_name = Some(name.into());
        self
    }

    /// seda-bus worker count (0 = number of cores).
    pub fn workers(mut self, n: usize) -> Self {
        self.workers = n;
        self
    }

    /// Runs before the bus is created.
    pub fn before_start<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&HashMap<String, String>) + Send + 'static,
    {
        self.before_start = Some(Box::new(f));
        self
    }

    /// Runs after the bus is running - register and start services here.
    pub fn on_bus_started<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&ServiceBus, &HashMap<String, String>) + Send + 'static,
    {
        self.on_bus_started = Some(Box::new(f));
        self
    }

    /// Runs at the start of shutdown, before the bus is stopped.
    pub fn on_stopping<F>(mut self, f: F) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        self.on_stopping = Some(Box::new(f));
        self
    }

    /// Load config, run the hooks, start the bus. Returns a handle to wait on
    /// and to shut down.
    pub fn launch(self, args: impl Iterator<Item = String>) -> DaemonHandle {
        let name = self
            .config_name
            .unwrap_or_else(|| "service-bus.config".to_string());
        let config = load_config(&name, args);

        if let Some(hook) = self.before_start {
            hook(&config);
        }

        let bus = ServiceBus::with_config(self.workers, config.clone());
        bus.start();

        if let Some(hook) = self.on_bus_started {
            hook(&bus, &config);
        }

        info!("service-bus daemon running.");
        DaemonHandle {
            bus,
            on_stopping: Mutex::new(self.on_stopping),
            done: Arc::new((Mutex::new(false), Condvar::new())),
        }
    }
}

/// A running daemon: wait on it, and shut it down.
pub struct DaemonHandle {
    bus: ServiceBus,
    on_stopping: Mutex<Option<Hook0>>,
    done: Arc<(Mutex<bool>, Condvar)>,
}

impl DaemonHandle {
    pub fn bus(&self) -> &ServiceBus {
        &self.bus
    }

    /// Block the calling thread until [`shutdown`](Self::shutdown) completes.
    pub fn wait(&self) {
        let (lock, cvar) = &*self.done;
        let mut done = lock.lock().unwrap();
        while !*done {
            done = cvar.wait(done).unwrap();
        }
    }

    /// Run `on_stopping`, gracefully stop the bus, and wake anything in
    /// [`wait`](Self::wait). Idempotent.
    pub fn shutdown(&self) {
        {
            let (lock, _) = &*self.done;
            if *lock.lock().unwrap() {
                return;
            }
        }
        info!("service-bus daemon shutting down...");
        if let Some(hook) = self.on_stopping.lock().unwrap().take() {
            hook();
        }
        let ok = self.bus.graceful_shutdown();
        info!("bus stopped={ok}");

        let (lock, cvar) = &*self.done;
        *lock.lock().unwrap() = true;
        cvar.notify_all();
    }
}

fn load_config(name: &str, args: impl Iterator<Item = String>) -> HashMap<String, String> {
    let mut config = HashMap::new();
    if let Ok(text) = fs::read_to_string(name) {
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((k, v)) = line.split_once('=') {
                config.insert(k.trim().to_string(), v.trim().to_string());
            }
        }
    }
    for arg in args {
        if let Some((k, v)) = arg.split_once('=') {
            config.insert(k.to_string(), v.to_string());
        }
    }
    config
}
