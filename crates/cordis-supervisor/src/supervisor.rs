//! One supervisor, with its own Tokio runtime, per plugin fiber.

use crate::task::{Task, TaskWorker};
use crate::{SupervisionConfig, budget};
use ash_flare::{
    RestartBackoff, RestartIntensity, SupervisorError, SupervisorHandle, SupervisorSpec,
};
use cordis::{Context, CordisError, ErrorCode, Fiber};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Supervises the long-running tasks of one plugin.
///
/// Created inside a plugin's `apply` with [`PluginSupervisor::start`], it
/// belongs to that plugin's fiber:
///
/// - It runs on a dedicated Tokio runtime, built by the code compiled into
///   the plugin. A plugin loaded from a dynamic library therefore uses its
///   own copy of Tokio and std consistently: timers and sockets work inside
///   tasks, and a panic is caught by the same std that raised it.
/// - When the fiber unloads (disposed, restarted, reconfigured, a
///   dependency vanished, or its library swapped for a new version) every
///   task is stopped, its `on_stop` hook runs, and the runtime is shut down
///   before the unload completes.
/// - If tasks fail faster than the restart budget allows, the supervisor
///   escalates: the fiber is restarted, up to the plugin-level budget,
///   after which it is left `Failed`.
#[derive(Clone)]
pub struct PluginSupervisor {
    inner: Arc<Inner>,
}

struct Inner {
    name: String,
    handle: SupervisorHandle<TaskWorker>,
    runtime: Mutex<Option<tokio::runtime::Runtime>>,
    /// Set before an intentional stop, so the escalation watcher stays quiet.
    stopping: AtomicBool,
    shutdown_timeout: Duration,
}

fn plugin_error(message: impl Into<String>) -> CordisError {
    CordisError::with_message(ErrorCode::Plugin, message)
}

fn supervisor_error(error: &SupervisorError) -> CordisError {
    plugin_error(error.to_string())
}

impl PluginSupervisor {
    /// Starts a supervisor owned by `ctx`'s fiber.
    ///
    /// Call it from the plugin's `apply`, then add tasks with
    /// [`supervise`](Self::supervise). Returns an error — failing the
    /// plugin — when its restart budget is exhausted, or the runtime cannot
    /// be built.
    ///
    /// # Errors
    ///
    /// `Plugin` when the plugin escalated more than
    /// `plugin_max_restarts` times within `plugin_restart_window`, or the
    /// Tokio runtime could not be created; `InactiveEffect` when `ctx` is
    /// not active.
    pub fn start(ctx: &Context, config: &SupervisionConfig) -> cordis::Result<Self> {
        let fiber = ctx.fiber()?;
        let uid = fiber
            .uid()
            .ok_or_else(|| CordisError::new(ErrorCode::InactiveEffect))?;
        let name = fiber.name();

        if let Some(escalations) = budget::exhausted(uid, config) {
            return Err(plugin_error(format!(
                "plugin `{name}` escalated {escalations} times within {:?} \
                 (limit {}); leaving it failed",
                config.plugin_restart_window, config.plugin_max_restarts
            )));
        }

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(config.worker_threads.max(1))
            .thread_name(format!("cordis-sup-{name}"))
            .enable_all()
            .build()
            .map_err(|error| plugin_error(format!("cannot start Tokio runtime: {error}")))?;

        let spec = SupervisorSpec::new(name.clone())
            .with_restart_strategy(config.strategy)
            .with_restart_intensity(RestartIntensity::new(
                config.max_restarts,
                config.restart_window.as_secs().max(1),
            ))
            .with_restart_backoff(RestartBackoff::exponential(
                config.backoff_initial,
                config.backoff_max,
            ))
            .with_shutdown_timeout(config.shutdown_timeout);
        let handle = {
            let _entered = runtime.enter();
            SupervisorHandle::start(spec)
        };

        let inner = Arc::new(Inner {
            name: name.clone(),
            handle: handle.clone(),
            runtime: Mutex::new(None),
            stopping: AtomicBool::new(false),
            shutdown_timeout: config.shutdown_timeout,
        });

        // Escalation watcher: the supervisor only stops on its own when its
        // restart intensity was exceeded.
        let watcher = Arc::clone(&inner);
        runtime.spawn(async move {
            watcher.handle.wait_stopped().await;
            if !watcher.stopping.load(Ordering::Acquire) {
                escalate(&watcher.name, uid, fiber);
            }
        });

        *inner
            .runtime
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(runtime);

        let owned = Arc::clone(&inner);
        if let Err(error) = ctx.effect_async(format!("supervisor {name}"), move || async move {
            owned.stop().await;
            Ok(())
        }) {
            // Not registered, so nothing else would stop it.
            cordis::utils::block_on(inner.stop());
            return Err(error);
        }

        Ok(Self { inner })
    }

    /// Starts `task` under this supervisor.
    ///
    /// # Errors
    ///
    /// `Plugin` when a task with the same id is already running, or the
    /// supervisor has stopped.
    pub fn supervise(&self, task: Task) -> cordis::Result<()> {
        let factory = TaskWorker::factory(&task);
        cordis::utils::block_on(self.inner.handle.start_child_with_signal(
            task.id,
            factory,
            task.policy,
        ))
        .map(|_id| ())
        .map_err(|error| supervisor_error(&error))
    }

    /// Stops the task `id`, running its `on_stop` hook. It is not restarted.
    ///
    /// # Errors
    ///
    /// `Plugin` when no such task is running or the supervisor has stopped.
    pub fn stop_task(&self, id: &str) -> cordis::Result<()> {
        cordis::utils::block_on(self.inner.handle.terminate_child(id))
            .map_err(|error| supervisor_error(&error))
    }

    /// Ids of the tasks currently supervised.
    ///
    /// # Errors
    ///
    /// `Plugin` when the supervisor has stopped.
    pub fn tasks(&self) -> cordis::Result<Vec<String>> {
        cordis::utils::block_on(self.inner.handle.which_children())
            .map(|children| children.into_iter().map(|child| child.id).collect())
            .map_err(|error| supervisor_error(&error))
    }

    /// The supervisor's name: its plugin's name.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Forgets the plugin-level escalations recorded for `fiber`, so a
    /// plugin left `Failed` by an exhausted budget can be restarted before
    /// its window expires.
    pub fn reset_restart_budget(fiber: &Fiber) {
        if let Some(uid) = fiber.uid() {
            budget::reset(uid);
        }
    }
}

impl Inner {
    /// Stops every task (waiting for their `on_stop` hooks), then the runtime.
    async fn stop(&self) {
        self.stopping.store(true, Ordering::Release);
        // Errs only if the supervisor already stopped (it escalated).
        let _stopped = self.handle.shutdown().await;

        let runtime = self
            .runtime
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(runtime) = runtime {
            // A runtime must not be shut down from an async context of its
            // own Tokio copy; a plain thread is always safe.
            let grace = self.shutdown_timeout;
            let joined = std::thread::Builder::new()
                .name(format!("cordis-sup-{}-stop", self.name))
                .spawn(move || runtime.shutdown_timeout(grace))
                .map(std::thread::JoinHandle::join);
            if joined.is_err() {
                tracing_free_warn(&self.name);
            }
        }
    }
}

/// Restarts `fiber` from a plain thread: Cordis lifecycle operations must
/// not run on a thread that may be inside one of the fiber's callbacks.
fn escalate(name: &str, uid: u64, fiber: Fiber) {
    budget::record_escalation(uid);
    let spawned = std::thread::Builder::new()
        .name(format!("cordis-sup-{name}-escalate"))
        .spawn(move || {
            // A restart that fails leaves the fiber `Failed` with the
            // reason (e.g. an exhausted budget) as its error.
            let _restarted = fiber.restart();
        });
    if spawned.is_err() {
        tracing_free_warn(name);
    }
}

/// Last-resort diagnostic when a helper thread cannot be spawned.
fn tracing_free_warn(name: &str) {
    eprintln!("cordis-supervisor: could not spawn a helper thread for plugin `{name}`");
}
