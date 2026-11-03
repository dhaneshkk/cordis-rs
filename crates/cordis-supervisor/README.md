# cordis-supervisor

Erlang/OTP-style supervision of plugin tasks for the
[cordis-rs](https://crates.io/crates/cordis-rs) plugin framework, built on
[ash-flare](https://github.com/dhaneshkk/ash-flare).

Cordis decides *when* a plugin runs: it starts the plugin once its
dependencies are available and unloads it when they disappear. This crate
keeps the plugin's background work alive *while* it runs.

```text
plugin fiber (Cordis)          restarted on escalation, then left Failed
└─ PluginSupervisor            one per plugin, on its own Tokio runtime
   ├─ Task "decoder"           restarted on error or panic, with backoff
   └─ Task "uploader"
```

## Usage

```rust,ignore
use cordis::{plugin_sync, Inject, PluginOutput};
use cordis_supervisor::{tokio, PluginSupervisor, SupervisionConfig, Task};
use std::time::Duration;

let zone = plugin_sync::<(), _>("camera_zone", Inject::default(), |ctx, _config| {
    let supervisor = PluginSupervisor::start(&ctx, &SupervisionConfig::default())?;
    supervisor.supervise(
        Task::new("decoder", |signal| async move {
            loop {
                tokio::select! {
                    () = signal.cancelled() => return Ok(()),
                    () = tokio::time::sleep(Duration::from_millis(40)) => {
                        // decode one frame; `?` or a panic restarts the task
                    }
                }
            }
        })
        .on_stop(|| async { /* flush buffers */ }),
    )?;
    Ok(PluginOutput::none())
});
```

Use the re-exported `cordis_supervisor::tokio` inside tasks, so they always
talk to the runtime they run on.

## Behaviour

| Event | What happens |
| --- | --- |
| A task returns an error or panics | It is restarted after an exponential backoff (`Permanent`/`Transient` policy). Its `on_stop` hook runs first. |
| Tasks fail faster than `max_restarts` per `restart_window` | The plugin's supervisor escalates: Cordis restarts the plugin's fiber, which starts a fresh supervisor. |
| The plugin escalates more than `plugin_max_restarts` per `plugin_restart_window` | The fiber is left `Failed` with an explanatory error. `PluginSupervisor::reset_restart_budget` clears it early. |
| The plugin unloads (disposed, reconfigured, a dependency vanished, library swapped) | Every task stops, its `on_stop` hook runs, and the runtime shuts down — all before the unload completes. |
| Another plugin crash-loops | Nothing: each plugin has its own supervisor, budget and runtime. |

## Configuration from the loader

With the `node` feature, `SupervisionConfig::from_node` reads settings from
an entry's config, so they can be changed (and hot-reloaded) per plugin:

```yaml
- id: zone-a
  name: camera_zone_v2
  config:
    supervision:
      strategy: one_for_one        # one_for_all | rest_for_one
      max_restarts: 5
      restart_window_secs: 10
      backoff_initial_ms: 100
      backoff_max_ms: 5000
      shutdown_timeout_ms: 2000    # keep the total under cordis run's 10 s grace
      worker_threads: 2
      plugin_max_restarts: 3
      plugin_restart_window_secs: 60
```

Unknown keys are rejected, to catch typos.

## Dynamic-library plugins and live swapping

A plugin compiled as a `cdylib` links its own copies of std and Tokio. The
supervisor, its runtime and its tasks are all created by code inside the
library, so they use the library's copies consistently: Tokio timers and
sockets work, and a panic is caught by the std that raised it.

To replace one plugin's code without restarting the process:

1. Build the new version under a new library name, e.g.
   `libcamera_zone_v2.so`, and put it next to `libcamera_zone_v1.so`.
2. Change the entry's `name` from `camera_zone_v1` to `camera_zone_v2` in
   the config file.

The loader stops the old fiber (its tasks stop and their `on_stop` hooks
run), loads the new library and starts it; other plugins are untouched.
Under `cordis run`, pass `--plugin-swap` so that adding the new file does not
restart the worker. The old library stays mapped until the process exits —
Rust code can never be safely unloaded — but nothing runs in it any more, so
each swap costs the old library's memory, not correctness.

State that must survive a swap has to live outside the plugin (a shared
service or storage): in-memory task state starts fresh, as with any restart.

## Rust version and publishing

Requires Rust 1.88 (ash-flare's MSRV), a documented exception to the
workspace's 1.85 policy. Not published to crates.io while it depends on
ash-flare through git (`publish = false`).
