# service-bus (Rust) — TODO

## Done (0.1.0)

- [x] `Service` trait + `ServiceCore` (name, atomic status, bus context);
      `ServiceStatus` enum.
- [x] `ServiceBus` (`Arc<Inner>`, `Clone`): register / `register_and_start_service(s)`
      / `start_all_registered`; `start_service` / `stop_service` /
      `unregister_service` (threaded).
- [x] Discovery: `get_service(name)`, `find_running_services(predicate)` (with
      `as_any` downcasting), `running_services()`, `is_registered` / `is_running`.
- [x] `await_running(timeout, &[names])`.
- [x] `pause` / `unpause` / `shutdown` / `graceful_shutdown`.
- [x] Health monitor thread: polls `status()`, publishes changes to listeners,
      restarts `Unstable` services.
- [x] Control commands via `headers["command"]` + `headers["service"]`.
- [x] `Daemon` builder + `DaemonHandle` (`wait` / `shutdown`).
- [x] Integration tests + `examples/pipeline.rs`; `README.md`, `DESIGN.md`.

## Next

- [ ] `restart()` on `ServiceBus` (java has it; here only per-service via the
      monitor).
- [ ] Real dependency ordering for **start** using `depends_on()` (topological sort).
- [ ] Factory registration (`register_factory(name, || Arc::new(S::new()))`) so
      `depends_on()` can auto-register and control-command `register` works.
- [ ] Optional `ctrlc` feature so `Daemon::launch` can install signal handlers
      itself (kept dep-free by default).
- [ ] Push-based status: an optional observer channel on `ServiceCore` so the
      monitor's 200 ms poll is not the only path (listeners would fire immediately).
- [ ] Readiness gate: hold delivery to a service until it reports `Running`.
- [ ] Health policy beyond "Unstable -> restart": restart backoff, give-up
      threshold, `Error` handling.
- [ ] Control-command responses (ack/nack back to the sender).
- [ ] Surface seda-bus `get_stats()` per service.
- [ ] Publish to crates.io (currently a path dependency on `../seda-bus-rust`).
