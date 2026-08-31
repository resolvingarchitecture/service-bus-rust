//! Service lifecycle management and discovery over a
//! [`seda_bus`](https://docs.rs/seda_bus).
//!
//! seda-bus moves envelopes between named channels and walks their routing
//! slips; `service-bus` gives you **services** as the unit of composition:
//! register them, start / stop / pause them, let them find each other, and
//! watch their health. Each service becomes one seda-bus channel keyed by its
//! name, with the service as that channel's consumer.
//!
//! A Rust port of the design in
//! [`service-bus-java`](https://github.com/resolvingarchitecture/service-bus-java).
//!
//! ```
//! use service_bus::{Service, ServiceBus, ServiceCore, ServiceStatus};
//! use seda_bus::Envelope;
//! use std::sync::{Arc, mpsc::channel};
//! use std::time::Duration;
//!
//! struct Echo { core: ServiceCore, tx: std::sync::mpsc::Sender<String> }
//! impl Service for Echo {
//!     fn name(&self) -> &str { self.core.name() }
//!     fn status(&self) -> ServiceStatus { self.core.status() }
//!     fn set_context(&self, ctx: service_bus::ServiceContext) { self.core.bind(ctx); }
//!     fn as_any(&self) -> &dyn std::any::Any { self }
//!     fn start(&self) -> bool { self.core.set_status(ServiceStatus::Running); true }
//!     fn handle(&self, env: &mut Envelope) -> bool {
//!         let _ = self.tx.send(String::from_utf8_lossy(&env.payload).into_owned());
//!         true
//!     }
//! }
//!
//! let bus = ServiceBus::new(2);
//! bus.start();
//! let (tx, rx) = channel();
//! bus.register_and_start_service(Arc::new(Echo { core: ServiceCore::new("echo"), tx }));
//! bus.await_running(Duration::from_secs(2), &["echo"]);
//!
//! bus.send(Envelope::new("echo", b"hello".to_vec()));
//! assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), "hello");
//! bus.graceful_shutdown();
//! ```

mod bus;
mod daemon;
mod service;

pub use bus::{BusStatus, ServiceBus, CONTROL_COMMANDS};
pub use daemon::{Daemon, DaemonHandle};
pub use service::{Service, ServiceContext, ServiceCore, ServiceStatus};

pub use seda_bus::Envelope;
