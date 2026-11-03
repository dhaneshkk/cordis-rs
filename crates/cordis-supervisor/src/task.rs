//! Supervised tasks: closures adapted to ash-flare workers.

use ash_flare::{RestartPolicy, ShutdownSignal, Worker};
use async_trait::async_trait;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Error a task returns to report failure; the task is then restarted
/// according to its [`RestartPolicy`].
pub type TaskError = Box<dyn std::error::Error + Send + Sync>;

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type RunFn = Arc<dyn Fn(ShutdownSignal) -> BoxFuture<Result<(), TaskError>> + Send + Sync>;
type StopFn = Arc<dyn Fn() -> BoxFuture<()> + Send + Sync>;

/// A long-running unit of work owned by a plugin.
///
/// `run` is called on every (re)start with a fresh [`ShutdownSignal`];
/// select on [`ShutdownSignal::cancelled`] to wind down cooperatively.
/// When the task is stopped, `run`'s future is dropped at its next await
/// point and then the `on_stop` hook runs: put cleanup (flushing, closing
/// connections) there. A panic in `run` is treated like a returned error.
pub struct Task {
    pub(crate) id: String,
    pub(crate) policy: RestartPolicy,
    pub(crate) run: RunFn,
    pub(crate) on_stop: Option<StopFn>,
}

impl Task {
    /// A [`RestartPolicy::Permanent`] task named `id`, unique within its
    /// plugin.
    pub fn new<F, Fut>(id: impl Into<String>, run: F) -> Self
    where
        F: Fn(ShutdownSignal) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), TaskError>> + Send + 'static,
    {
        Self {
            id: id.into(),
            policy: RestartPolicy::Permanent,
            run: Arc::new(move |signal| Box::pin(run(signal))),
            on_stop: None,
        }
    }

    /// When the task is restarted: `Permanent` (always), `Transient` (only
    /// after a failure) or `Temporary` (never).
    #[must_use]
    pub fn policy(mut self, policy: RestartPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Cleanup run after every stop of the task, whether it finished,
    /// failed, panicked, or was asked to stop. Keep it within the plugin's
    /// `shutdown_timeout`.
    #[must_use]
    pub fn on_stop<F, Fut>(mut self, on_stop: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_stop = Some(Arc::new(move || Box::pin(on_stop())));
        self
    }

    /// The task's id.
    pub fn id(&self) -> &str {
        &self.id
    }
}

impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Task")
            .field("id", &self.id)
            .field("policy", &self.policy)
            .field("on_stop", &self.on_stop.is_some())
            .finish_non_exhaustive()
    }
}

/// A task failure, as reported to ash-flare.
#[derive(Debug)]
pub struct TaskFailure(String);

impl fmt::Display for TaskFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TaskFailure {}

/// The one worker type every plugin supervisor runs.
pub struct TaskWorker {
    run: RunFn,
    on_stop: Option<StopFn>,
    signal: ShutdownSignal,
}

impl TaskWorker {
    pub(crate) fn factory(task: &Task) -> impl Fn(ShutdownSignal) -> Self + Send + Sync + 'static {
        let run = Arc::clone(&task.run);
        let on_stop = task.on_stop.clone();
        move |signal| Self {
            run: Arc::clone(&run),
            on_stop: on_stop.clone(),
            signal,
        }
    }
}

#[async_trait]
impl Worker for TaskWorker {
    type Error = TaskFailure;

    async fn run(&mut self) -> Result<(), Self::Error> {
        (self.run)(self.signal.clone())
            .await
            .map_err(|error| TaskFailure(error.to_string()))
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        if let Some(on_stop) = &self.on_stop {
            on_stop().await;
        }
        Ok(())
    }
}
