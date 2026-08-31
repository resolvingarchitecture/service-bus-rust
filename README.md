<div align="center">
  <h1>service-bus (Rust)</h1>
  <p><strong>Resolving Architecture &mdash; Clarity in Design</strong></p>
  <p>Service lifecycle management and discovery over a seda-bus.</p>
</div>

`service-bus` sits on top of
[`seda-bus`](https://github.com/resolvingarchitecture/seda-bus-rust): seda-bus moves
envelopes between named channels and walks their routing slips; `service-bus` gives
you **services** as the unit of composition &mdash; register them, start/stop/pause
them, let them find each other, and watch their health.

Each service becomes one seda-bus channel keyed by its name, with the service as
that channel's consumer. This is a Rust port of the design in
[`service-bus-java`](https://github.com/resolvingarchitecture/service-bus-java).

```rust
use service_bus::{Service, ServiceBus, ServiceContext, ServiceCore, ServiceStatus};
use seda_bus::Envelope;
use std::sync::Arc;
use std::time::Duration;

struct EchoService { core: ServiceCore }

impl Service for EchoService {
    fn name(&self) -> &str { self.core.name() }
    fn status(&self) -> ServiceStatus { self.core.status() }
    fn set_context(&self, ctx: ServiceContext) { self.core.bind(ctx); }
    fn as_any(&self) -> &dyn std::any::Any { self }
    fn start(&self) -> bool { self.core.set_status(ServiceStatus::Running); true }
    fn handle(&self, env: &mut Envelope) -> bool {
        println!("{}", String::from_utf8_lossy(&env.payload));
        true
    }
}

let bus = ServiceBus::new(4);
bus.start();

bus.register_and_start_service(Arc::new(EchoService { core: ServiceCore::new("echo") }));
bus.await_running(Duration::from_secs(5), &["echo"]);

// discovery
let echo = bus.get_service("echo");
let transports = bus.find_running_services(|s| s.as_any().is::<MyTransport>());

// send
bus.send(Envelope::new("echo", b"hello".to_vec()));
bus.send_with_callback(Envelope::new("echo", b"hi".to_vec()), |e| println!("done {}", e.id));

bus.graceful_shutdown();
```

## `Service` and `ServiceCore`

Rust has no inheritance, so instead of a `BaseService` superclass you embed a
`ServiceCore` (name + atomic status + bus context) and forward the trait methods to
it. `handle(&self, ...)` &mdash; seda-bus consumers are `Fn` &mdash; so per-service
state lives behind `Mutex` / atomics.

## As a daemon

```rust
use service_bus::{Daemon, ServiceBus};

let handle = Daemon::new()
    .config_name("my.config")
    .on_bus_started(|bus: &ServiceBus, _cfg| {
        // bus.register_and_start_services(vec![Arc::new(FooService::new())]);
        bus.await_running(std::time::Duration::from_secs(10), &[]);
    })
    .launch(std::env::args());

// std has no signal handling - wire SIGINT/SIGTERM (e.g. the `ctrlc` crate) to:
//   handle.shutdown()
handle.wait();
```

## API

| area       | methods                                                                              |
|------------|------------------------------------------------------------------------------------|
| register   | `register(Arc<dyn Service>)`, `register_and_start_service(s)`, `register_and_start_services(vec)` |
| lifecycle  | `start_service`, `stop_service`, `start_all_registered`, `pause`/`unpause`, `shutdown`/`graceful_shutdown` |
| discovery  | `get_service(name)`, `find_running_services(pred)`, `running_services()`, `is_registered`/`is_running` |
| wait       | `await_running(timeout, &[names])`                                                  |
| health     | `get_service_status(name)`, `get_service_statuses()`, `add_service_status_listener` |
| bus        | `status()`, `add_bus_status_listener`                                              |
| control    | `send` an `Envelope` with `headers["command"]` (`start`/`stop`/`pause`/...) and `headers["service"]` |

## Behaviour

- **Threaded start/stop** &mdash; `start_service` spawns a thread; join with
  `await_running`.
- **Advisory dependencies** &mdash; `Service::depends_on()` is checked at registration
  (warns); order your `register_and_start_services(...)` call.
- **Health monitor** &mdash; a background thread polls each running service's
  `status()`, publishes changes to listeners, and restarts a service that reports
  `ServiceStatus::Unstable`. (Java pushes status; Rust polls &mdash; `get_service_status`
  reads the service live so callers never see stale values.)
- **Dead letters** &mdash; use the underlying seda-bus:
  `bus.seda_bus().set_dead_letter_channel(source, dlq)`.

## Build

```sh
cargo test          # needs ../seda-bus-rust (path dependency)
cargo run --example pipeline
cargo clippy --all-targets
```

## Reference

- [`DESIGN.md`](DESIGN.md)
- [`TODO.md`](TODO.md)
