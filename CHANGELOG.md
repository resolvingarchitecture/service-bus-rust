# Changelog

## 0.1.0

Initial release. A Rust port of `service-bus-java`'s design onto `seda-bus-rust`.

- `Service` trait + `ServiceCore` composable helper + `ServiceStatus`.
- `ServiceBus` (`Arc<Inner>`, `Clone`): register, start/stop/pause (threaded
  starts), discovery (`get_service`, `find_running_services` with `as_any`
  downcasting), `await_running`, a health-monitor thread that publishes status
  changes and restarts `Unstable` services, control commands via envelope headers.
- `Daemon` builder with closure hooks (`config_name` / `before_start` /
  `on_bus_started` / `on_stopping`) + `DaemonHandle` (`wait` / `shutdown`).
- `examples/pipeline.rs`.
- Path dependency on `seda_bus` (`seda-bus-rust`); only other dependency is `log`.

This replaces the abandoned 2019-era skeleton on `origin/master` (which targeted
`seda_bus 0.1.x` and `ra_common`).
