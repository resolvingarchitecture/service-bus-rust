//! The unit of composition on the bus: a service.

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};

use seda_bus::{Bus, Envelope};

/// Lifecycle state of a service on the bus.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServiceStatus {
    NotInitialized,
    Starting,
    Running,
    Paused,
    /// Degraded / self-reported broken - the bus restarts it.
    Unstable,
    ShuttingDown,
    Shutdown,
    Error,
}

impl ServiceStatus {
    fn to_u8(self) -> u8 {
        match self {
            ServiceStatus::NotInitialized => 0,
            ServiceStatus::Starting => 1,
            ServiceStatus::Running => 2,
            ServiceStatus::Paused => 3,
            ServiceStatus::Unstable => 4,
            ServiceStatus::ShuttingDown => 5,
            ServiceStatus::Shutdown => 6,
            ServiceStatus::Error => 7,
        }
    }
    fn from_u8(v: u8) -> ServiceStatus {
        match v {
            1 => ServiceStatus::Starting,
            2 => ServiceStatus::Running,
            3 => ServiceStatus::Paused,
            4 => ServiceStatus::Unstable,
            5 => ServiceStatus::ShuttingDown,
            6 => ServiceStatus::Shutdown,
            7 => ServiceStatus::Error,
            _ => ServiceStatus::NotInitialized,
        }
    }
}

/// Handed to a service so it can read config and send envelopes back onto the bus.
#[derive(Clone)]
pub struct ServiceContext {
    seda: Bus,
    config: Arc<HashMap<String, String>>,
}

impl ServiceContext {
    pub(crate) fn new(seda: Bus, config: Arc<HashMap<String, String>>) -> Self {
        ServiceContext { seda, config }
    }

    pub fn config(&self) -> &HashMap<String, String> {
        &self.config
    }

    /// Publish an envelope. (Control-command headers are not interpreted on this
    /// path - use it for ordinary routing.)
    pub fn send(&self, env: Envelope) -> bool {
        self.seda.publish(env, None)
    }

    pub fn send_with_callback<F>(&self, env: Envelope, on_complete: F) -> bool
    where
        F: FnOnce(&Envelope) + Send + 'static,
    {
        self.seda.publish_with_callback(env, None, on_complete)
    }
}

/// A service on the [`ServiceBus`](crate::ServiceBus).
///
/// Each service becomes one seda-bus channel keyed by [`Service::name`], with
/// the service as that channel's consumer. `handle` takes `&self` (seda-bus
/// consumers are `Fn`), so any per-service state lives behind interior
/// mutability - see [`ServiceCore`] for the common bits.
pub trait Service: Send + Sync {
    /// Unique name: the registration key, the channel name, the routing target.
    fn name(&self) -> &str;

    /// Names of services that must start before this one. Advisory ordering.
    fn depends_on(&self) -> Vec<String> {
        Vec::new()
    }

    /// Handle an envelope routed to this service. Return `false` to nack.
    fn handle(&self, envelope: &mut Envelope) -> bool;

    fn start(&self) -> bool {
        true
    }
    fn stop(&self) -> bool {
        true
    }
    fn pause(&self) {}
    fn unpause(&self) {}

    fn status(&self) -> ServiceStatus {
        ServiceStatus::Running
    }

    /// Wired by [`ServiceBus::register`]. Services that embed a [`ServiceCore`]
    /// forward this to [`ServiceCore::bind`].
    fn set_context(&self, _ctx: ServiceContext) {}

    /// For typed discovery: `bus.find_running_services(|s| s.as_any().is::<Foo>())`
    /// and `arc.as_any().downcast_ref::<Foo>()`.
    fn as_any(&self) -> &dyn Any;
}

/// Reusable service internals: a name, an atomic status, and the bus context.
/// Embed one and delegate the trait methods to it.
///
/// ```ignore
/// struct EchoService { core: ServiceCore }
///
/// impl Service for EchoService {
///     fn name(&self) -> &str { self.core.name() }
///     fn status(&self) -> ServiceStatus { self.core.status() }
///     fn set_context(&self, ctx: ServiceContext) { self.core.bind(ctx); }
///     fn as_any(&self) -> &dyn std::any::Any { self }
///     fn start(&self) -> bool { self.core.set_status(ServiceStatus::Running); true }
///     fn handle(&self, env: &mut Envelope) -> bool { /* ... */ true }
/// }
/// ```
pub struct ServiceCore {
    name: String,
    status: AtomicU8,
    ctx: OnceLock<ServiceContext>,
}

impl ServiceCore {
    pub fn new(name: impl Into<String>) -> ServiceCore {
        ServiceCore {
            name: name.into(),
            status: AtomicU8::new(ServiceStatus::NotInitialized.to_u8()),
            ctx: OnceLock::new(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn status(&self) -> ServiceStatus {
        ServiceStatus::from_u8(self.status.load(Ordering::Acquire))
    }

    pub fn set_status(&self, status: ServiceStatus) {
        self.status.store(status.to_u8(), Ordering::Release);
    }

    pub fn bind(&self, ctx: ServiceContext) {
        let _ = self.ctx.set(ctx);
    }

    pub fn context(&self) -> Option<&ServiceContext> {
        self.ctx.get()
    }

    /// Send an envelope through the bus (no-op returning `false` before the
    /// service is registered).
    pub fn send(&self, env: Envelope) -> bool {
        match self.ctx.get() {
            Some(ctx) => ctx.send(env),
            None => false,
        }
    }
}
