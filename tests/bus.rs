use std::any::Any;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use seda_bus::Envelope;
use service_bus::{Service, ServiceBus, ServiceContext, ServiceCore, ServiceStatus};

struct Recorder {
    core: ServiceCore,
    seen: Mutex<Vec<String>>,
}

impl Recorder {
    fn new(name: &str) -> Arc<Recorder> {
        Arc::new(Recorder {
            core: ServiceCore::new(name),
            seen: Mutex::new(Vec::new()),
        })
    }
}

impl Service for Recorder {
    fn name(&self) -> &str {
        self.core.name()
    }
    fn status(&self) -> ServiceStatus {
        self.core.status()
    }
    fn set_context(&self, ctx: ServiceContext) {
        self.core.bind(ctx);
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn start(&self) -> bool {
        self.core.set_status(ServiceStatus::Running);
        true
    }
    fn stop(&self) -> bool {
        self.core.set_status(ServiceStatus::Shutdown);
        true
    }
    fn handle(&self, env: &mut Envelope) -> bool {
        self.seen.lock().unwrap().push(env.id.clone());
        true
    }
}

/// A protocol-adapter-shaped service, to exercise typed discovery.
struct Transport {
    core: ServiceCore,
}
impl Transport {
    fn new(name: &str) -> Arc<Transport> {
        Arc::new(Transport {
            core: ServiceCore::new(name),
        })
    }
}
impl Service for Transport {
    fn name(&self) -> &str {
        self.core.name()
    }
    fn status(&self) -> ServiceStatus {
        self.core.status()
    }
    fn set_context(&self, ctx: ServiceContext) {
        self.core.bind(ctx);
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn start(&self) -> bool {
        self.core.set_status(ServiceStatus::Running);
        true
    }
    fn handle(&self, _env: &mut Envelope) -> bool {
        true
    }
}

#[test]
fn register_start_discover_await() {
    let bus = ServiceBus::new(2);
    bus.start();
    let rec = Recorder::new("recorder");

    assert!(bus.register_and_start_service(rec.clone()));
    assert!(bus.await_running(Duration::from_secs(2), &["recorder"]));

    assert!(bus.is_registered("recorder"));
    assert!(bus.is_running("recorder"));
    assert!(bus.get_service("recorder").is_some());
    assert_eq!(
        bus.get_service_status("recorder"),
        Some(ServiceStatus::Running)
    );
    let found = bus.find_running_services(|s| s.as_any().is::<Recorder>());
    assert_eq!(found.len(), 1);

    bus.graceful_shutdown();
}

#[test]
fn routes_envelope_and_fires_callback() {
    let bus = ServiceBus::new(2);
    bus.start();
    let rec = Recorder::new("recorder");
    bus.register_and_start_service(rec.clone());
    bus.await_running(Duration::from_secs(2), &["recorder"]);

    let (tx, rx) = channel::<String>();
    let env = Envelope::new("recorder", b"hi".to_vec());
    let id = env.id.clone();
    bus.send_with_callback(env, move |e| {
        let _ = tx.send(e.id.clone());
    });

    assert_eq!(rx.recv_timeout(Duration::from_secs(3)).unwrap(), id);
    assert!(rec.seen.lock().unwrap().contains(&id));

    bus.graceful_shutdown();
}

#[test]
fn routing_slip_walks_services_in_order() {
    let bus = ServiceBus::new(2);
    bus.start();
    let a = Recorder::new("a");
    let b = Recorder::new("b");
    bus.register_and_start_services(vec![a.clone(), b.clone()]);
    bus.await_running(Duration::from_secs(2), &["a", "b"]);

    let (tx, rx) = channel::<()>();
    let env = Envelope::new("a", b"x".to_vec()).with_slip(["b"]);
    let id = env.id.clone();
    bus.send_with_callback(env, move |_| {
        let _ = tx.send(());
    });
    rx.recv_timeout(Duration::from_secs(3)).unwrap();

    assert!(a.seen.lock().unwrap().contains(&id));
    assert!(b.seen.lock().unwrap().contains(&id));

    bus.graceful_shutdown();
}

#[test]
fn typed_discovery_finds_all_transports() {
    let bus = ServiceBus::new(2);
    bus.start();
    bus.register_and_start_services(vec![
        Transport::new("i2p"),
        Transport::new("tor"),
        Recorder::new("recorder"),
    ]);
    bus.await_running(Duration::from_secs(2), &[]);

    let mut names: Vec<String> = bus
        .find_running_services(|s| s.as_any().is::<Transport>())
        .iter()
        .map(|s| s.name().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["i2p".to_string(), "tor".to_string()]);

    bus.graceful_shutdown();
}

#[test]
fn pause_and_unpause() {
    let bus = ServiceBus::new(2);
    bus.start();
    let rec = Recorder::new("recorder");
    bus.register_and_start_service(rec);
    bus.await_running(Duration::from_secs(2), &["recorder"]);

    assert!(bus.pause());
    assert_eq!(bus.status(), service_bus::BusStatus::Paused);
    assert!(bus.unpause());
    assert_eq!(bus.status(), service_bus::BusStatus::Running);

    bus.graceful_shutdown();
}

#[test]
fn unstable_service_is_restarted_by_the_monitor() {
    struct Flaky {
        core: ServiceCore,
        starts: Arc<Mutex<u32>>,
    }
    impl Service for Flaky {
        fn name(&self) -> &str {
            self.core.name()
        }
        fn status(&self) -> ServiceStatus {
            self.core.status()
        }
        fn set_context(&self, ctx: ServiceContext) {
            self.core.bind(ctx);
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn start(&self) -> bool {
            *self.starts.lock().unwrap() += 1;
            self.core.set_status(ServiceStatus::Running);
            true
        }
        fn stop(&self) -> bool {
            true
        }
        fn handle(&self, _env: &mut Envelope) -> bool {
            true
        }
    }

    let bus = ServiceBus::new(2);
    bus.start();
    let starts = Arc::new(Mutex::new(0));
    let flaky = Arc::new(Flaky {
        core: ServiceCore::new("flaky"),
        starts: starts.clone(),
    });
    bus.register_and_start_service(flaky.clone());
    bus.await_running(Duration::from_secs(2), &["flaky"]);
    assert_eq!(*starts.lock().unwrap(), 1);

    flaky.core.set_status(ServiceStatus::Unstable);
    std::thread::sleep(Duration::from_millis(700)); // one monitor tick + restart
    assert_eq!(*starts.lock().unwrap(), 2);

    bus.graceful_shutdown();
}

#[test]
fn control_command_over_the_bus() {
    let bus = ServiceBus::new(2);
    bus.start();
    let rec = Recorder::new("recorder");
    bus.register(rec); // registered, not started
    assert!(!bus.is_running("recorder"));

    let env = Envelope::new("recorder", Vec::new())
        .with_header("command", "start")
        .with_header("service", "recorder");
    bus.send(env);
    assert!(bus.await_running(Duration::from_secs(2), &["recorder"]));

    bus.graceful_shutdown();
}

// keep Sender import used on all cfgs
#[allow(dead_code)]
fn _unused(_t: Sender<()>) {}
