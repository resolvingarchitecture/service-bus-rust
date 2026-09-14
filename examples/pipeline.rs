//! Two services, a routing slip between them, and discovery.
//!
//!     cargo run --example pipeline

use std::any::Any;
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::time::Duration;

use seda_bus::{envelope_payload, make_envelope, set_payload, Envelope};
use service_bus::{Service, ServiceBus, ServiceContext, ServiceCore, ServiceStatus};

struct Uppercase {
    core: ServiceCore,
}
impl Service for Uppercase {
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
    fn handle(&self, env: &mut Envelope) -> bool {
        let upper = envelope_payload(env)
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_uppercase();
        set_payload(env, upper.into());
        true
    }
}

struct Printer {
    core: ServiceCore,
    tx: std::sync::mpsc::Sender<String>,
}
impl Service for Printer {
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
    fn handle(&self, env: &mut Envelope) -> bool {
        let text = envelope_payload(env).and_then(|v| v.as_str()).unwrap_or_default().to_string();
        let from = env.header("from").and_then(|v| v.as_str()).unwrap_or("?");
        println!("  [{from}] {text}");
        let _ = self.tx.send(text);
        true
    }
}

fn main() {
    let bus = ServiceBus::new(0);
    bus.start();

    let (tx, rx) = channel();
    bus.register_and_start_services(vec![
        Arc::new(Uppercase {
            core: ServiceCore::new("uppercase"),
        }),
        Arc::new(Printer {
            core: ServiceCore::new("printer"),
            tx,
        }),
    ]);
    bus.await_running(Duration::from_secs(5), &[]);

    println!("running: {:?}", bus.running_service_names());
    println!(
        "discovered printers: {}",
        bus.find_running_services(|s| s.as_any().is::<Printer>()).len()
    );

    for word in ["alpha", "bravo", "charlie"] {
        let mut env = make_envelope("uppercase", Some(word.into()), ["printer".to_string()]);
        env.set_header("from", "demo".into());
        bus.send(env);
    }
    for _ in 0..3 {
        let _ = rx.recv_timeout(Duration::from_secs(2));
    }

    bus.graceful_shutdown();
}
