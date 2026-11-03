//! Erlang/OTP-style supervision of plugin tasks for
//! [cordis-rs](https://crates.io/crates/cordis-rs), built on
//! [ash-flare](https://github.com/dhaneshkk/ash-flare).
//!
//! Cordis decides *when* a plugin runs: it starts the plugin once its
//! dependencies are available and unloads it when they disappear. This crate
//! keeps the plugin's background work alive *while* it runs: a task that
//! fails or panics is restarted with backoff, and a plugin whose tasks keep
//! failing is restarted as a whole, then left `Failed`.
//!
//! ```no_run
//! use cordis::{plugin_sync, Inject, PluginOutput};
//! use cordis_supervisor::{tokio, PluginSupervisor, SupervisionConfig, Task};
//! use std::time::Duration;
//!
//! let zone = plugin_sync::<(), _>("camera_zone", Inject::default(), |ctx, _config| {
//!     let supervisor = PluginSupervisor::start(&ctx, &SupervisionConfig::default())?;
//!     supervisor.supervise(
//!         Task::new("decoder", |signal| async move {
//!             loop {
//!                 tokio::select! {
//!                     () = signal.cancelled() => return Ok(()),
//!                     () = tokio::time::sleep(Duration::from_millis(40)) => {
//!                         // decode one frame; `?` or a panic restarts the task
//!                     }
//!                 }
//!             }
//!         })
//!         .on_stop(|| async { /* flush buffers */ }),
//!     )?;
//!     Ok(PluginOutput::none())
//! });
//! # let _ = zone;
//! ```
//!
//! # One supervisor and runtime per plugin
//!
//! Each [`PluginSupervisor`] runs on its own small Tokio runtime and belongs
//! to its plugin's fiber. A crash loop in one plugin cannot exhaust another
//! plugin's restart budget, and unloading a plugin stops exactly its tasks.
//!
//! # Dynamic-library plugins and live swapping
//!
//! A plugin compiled as a `cdylib` statically links its own copies of std
//! and Tokio. Because the supervisor, its runtime and the tasks are all
//! created by code inside the library, they consistently use the library's
//! copies: Tokio timers and sockets work in tasks, and panics are caught by
//! the std that raised them.
//!
//! Combined with the loader, this allows replacing one plugin's code
//! without restarting the process: build the new version under a new
//! library name (for example `libcamera_zone_v2.so`), place it next to the
//! old one, and change the entry's `name` in the config file. The loader
//! stops the old fiber (its tasks stop and their `on_stop` hooks run) and
//! starts the new one. The old library stays mapped — Rust code can never
//! be safely unloaded — but nothing runs in it any more. With `cordis run`,
//! pass `--plugin-swap` so that adding a new library file does not restart
//! the worker.
//!
//! # Rust version
//!
//! This crate requires Rust 1.88 (ash-flare's MSRV), a documented exception
//! to the workspace's 1.85 policy. It is not published to crates.io while it
//! depends on ash-flare through git.

#![deny(unsafe_code)]
#![warn(missing_docs)]

mod budget;
mod config;
mod supervisor;
mod task;

pub use ash_flare::{RestartPolicy, RestartStrategy, ShutdownSignal};
pub use config::SupervisionConfig;
pub use supervisor::PluginSupervisor;
pub use task::{Task, TaskError, TaskFailure, TaskWorker};
/// The Tokio this crate's runtimes are built with. Tasks — especially in
/// dynamic-library plugins — should use it rather than a separately added
/// `tokio` dependency, so they always talk to the runtime they run on.
pub use tokio;
