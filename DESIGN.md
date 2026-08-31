# service-bus (Rust) — Design

A port of [`service-bus-java`](https://github.com/resolvingarchitecture/service-bus-java)'s
design onto [`seda-bus-rust`](https://github.com/resolvingarchitecture/seda-bus-rust).
Same model, idiomatic to Rust.

## Role

`seda-bus` is a transport: named channels, a shared thread pool, bounded queues,
the routing-slip engine. It has no notion of a "service".

`service-bus` adds that notion and the management around it:

- **composition** — a `Service` is the unit; each becomes one channel + its
  consumer;
- **lifecycle** — register, start, stop, pause, restart, per service and for the
  whole bus;
- **discovery** — look services up by name, by predicate, or by concrete type
  (`as_any` downcast);
- **health** — a monitor thread watches each service's `status()`, publishes
  changes, and restarts an `Unstable` one;
- **remote control** — an `Envelope` carrying `headers["command"]` acts on a
  service;
- **a reusable `Daemon`** host.

## Model

    ServiceBus(Arc<Inner>)         // cheap to clone
      seda           seda_bus::Bus
      config         Arc<HashMap<String,String>>
      registered     RwLock<HashMap<String, Arc<dyn Service>>>
      running        RwLock<HashMap<String, Arc<dyn Service>>>
      statuses       RwLock<HashMap<String, ServiceStatus>>   // monitor's diff cache
      svc_listeners / bus_listeners
      monitor        JoinHandle  (+ monitor_stop: AtomicBool)

    register(service):
      service.set_context(ServiceContext { seda, config })
      seda.channel(name, ChannelConfig::default())
      seda.subscribe(name, move |env| service.handle(env))

    start_service(name):  thread::spawn -> service.start() -> running.insert(name, service)
    send(env):            if headers["command"] in CONTROL_COMMANDS -> act;  seda.publish(env)

"name" is the registration key, the channel name, and the value a routing slip
carries (`Envelope::to` / `Envelope::slip`).

## No inheritance: `Service` + `ServiceCore`

Java's `BaseService` superclass becomes a trait plus a composable helper:

- `Service` — `name`, `handle(&self, &mut Envelope) -> bool`, `start`/`stop`/`pause`/
  `unpause`, `status`, `set_context`, `as_any`. `handle` is `&self` (seda-bus
  consumers are `Fn`), so state lives behind `Mutex` / atomics.
- `ServiceCore` — `name: String`, `status: AtomicU8`, `ctx: OnceLock<ServiceContext>`.
  A concrete service embeds one and forwards `name()` / `status()` / `set_context()`
  to it, and calls `core.set_status(...)` from its `start` / `stop` / on failure.

## Differences from `service-bus-java` (and why)

| java | here | reason |
|------|------|--------|
| reflective `Class.forName` registration | pass an `Arc<dyn Service>` | no reflection; the value carries its own `name` |
| dependency-ordered auto-registration | `depends_on()` is advisory (warns) | can't build an unknown dependency without a factory |
| `BaseService` superclass | `Service` trait + embedded `ServiceCore` | no inheritance |
| push `serviceStatusChanged` -> listeners | a **monitor thread** polls `status()` every 200 ms and diffs | no callback plumbing through `Arc<dyn Service>`; `get_service_status` reads the service live so it is never stale |
| `findRunningServices(Class)` | `find_running_services(|s| s.as_any().is::<T>())` | Rust downcasting via `Any` |
| `ControlCommand` enum on the envelope | `headers["command"]` + `headers["service"]` | seda-bus envelopes carry a `HashMap<String,String>`, not a typed command path |
| `Daemon` with overridable methods | `Daemon` builder with closure hooks + `DaemonHandle` | no inheritance; std has no signal handling, so the app wires SIGINT/SIGTERM to `handle.shutdown()` |
| routing slip is a LIFO stack | seda-bus-rust slip is FIFO (`VecDeque`) | property of the underlying bus; the router pattern is unchanged |

## Threading

`start_service` / `stop_service` spawn a `std::thread` running `service.start()` /
`.stop()`. `await_running(timeout, &[names])` polls the `running` map. The monitor
thread (`service-bus-monitor`) runs while the bus is up and is joined on shutdown.

`ServiceBus` is `Arc<Inner>` and `Clone` — worker closures and spawned threads hold
their own clone.

## Daemon

`Daemon::new().config_name(..).before_start(..).on_bus_started(..).on_stopping(..)
.launch(args)` returns a `DaemonHandle`. `launch` loads `key=value` config
(file + args), runs `before_start`, builds and starts the `ServiceBus`, runs
`on_bus_started`. `DaemonHandle::wait()` parks on a `Condvar`;
`DaemonHandle::shutdown()` runs `on_stopping`, `graceful_shutdown()`s the bus, and
wakes the waiters. The std library has no signal handling, so a deployment wires
SIGINT/SIGTERM to `shutdown()` itself.

## Not here

- No priority between services (that is seda-bus per-stage config).
- No distributed registry — one process, one bus.
- No hot reload — restart re-runs `start()` on the same value.
