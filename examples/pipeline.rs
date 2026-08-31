//! Two services, a routing slip between them, and discovery.
//!
//!     cargo run --example pipeline

use std::any::Any;
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::time::Duration;

use seda_bus::Envelope;
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
        env.payload.make_ascii_uppercase();
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
        let text = String::from_utf8_lossy(&env.payload).into_owned();
        println!("  [{}] {text}", env.headers.get("from").map(String::as_str).unwrap_or("?"));
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
        bus.send(
            Envelope::new("uppercase", word.as_bytes().to_vec())
                .with_slip(["printer"])
                .with_header("from", "demo"),
        );
    }
    for _ in 0..3 {
        let _ = rx.recv_timeout(Duration::from_secs(2));
    }

    bus.graceful_shutdown();
}
