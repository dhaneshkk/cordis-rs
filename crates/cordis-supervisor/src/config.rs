//! Supervision settings for one plugin.

use ash_flare::RestartStrategy;
use std::time::Duration;

/// How a plugin's tasks are supervised.
///
/// Two levels, as in OTP:
///
/// 1. **Task level** (ash-flare): a failed or panicking task is restarted
///    according to [`strategy`](Self::strategy), with exponential backoff.
///    More than [`max_restarts`](Self::max_restarts) restarts within
///    [`restart_window`](Self::restart_window) makes the plugin's supervisor
///    give up (escalate).
/// 2. **Plugin level** (Cordis): an escalation restarts the plugin's fiber,
///    which re-runs the plugin and starts a fresh supervisor. More than
///    [`plugin_max_restarts`](Self::plugin_max_restarts) escalations within
///    [`plugin_restart_window`](Self::plugin_restart_window) leave the fiber
///    `Failed` with an explanatory error instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisionConfig {
    /// Which tasks restart when one fails.
    pub strategy: RestartStrategy,
    /// Task restarts allowed within [`restart_window`](Self::restart_window).
    pub max_restarts: usize,
    /// Sliding window for [`max_restarts`](Self::max_restarts); whole seconds
    /// (ash-flare's resolution).
    pub restart_window: Duration,
    /// Delay before the first restart of a failing task.
    pub backoff_initial: Duration,
    /// Upper bound of the doubling restart delay.
    pub backoff_max: Duration,
    /// Time each task gets to stop (including its `on_stop` hook) before it
    /// is aborted. Keep the plugin's total well under `cordis run`'s 10 s
    /// worker shutdown grace period.
    pub shutdown_timeout: Duration,
    /// Worker threads of the plugin's own Tokio runtime.
    pub worker_threads: usize,
    /// Plugin restarts (escalations) allowed within
    /// [`plugin_restart_window`](Self::plugin_restart_window).
    pub plugin_max_restarts: usize,
    /// Sliding window for [`plugin_max_restarts`](Self::plugin_max_restarts).
    pub plugin_restart_window: Duration,
}

impl Default for SupervisionConfig {
    fn default() -> Self {
        Self {
            strategy: RestartStrategy::OneForOne,
            max_restarts: 3,
            restart_window: Duration::from_secs(5),
            backoff_initial: Duration::from_millis(100),
            backoff_max: Duration::from_secs(5),
            shutdown_timeout: Duration::from_secs(2),
            worker_threads: 2,
            plugin_max_restarts: 3,
            plugin_restart_window: Duration::from_secs(60),
        }
    }
}

#[cfg(feature = "node")]
mod node {
    use super::SupervisionConfig;
    use ash_flare::RestartStrategy;
    use cordis::{CordisError, ErrorCode};
    use cordis_include::Node;
    use std::time::Duration;

    const KEYS: [&str; 9] = [
        "strategy",
        "max_restarts",
        "restart_window_secs",
        "backoff_initial_ms",
        "backoff_max_ms",
        "shutdown_timeout_ms",
        "worker_threads",
        "plugin_max_restarts",
        "plugin_restart_window_secs",
    ];

    fn invalid(message: String) -> CordisError {
        CordisError::with_message(ErrorCode::InvalidConfig, message)
    }

    fn count(node: &Node, key: &str) -> Result<Option<u64>, CordisError> {
        let value = &node[key];
        if value.is_null() {
            return Ok(None);
        }
        value
            .as_i64()
            .and_then(|number| u64::try_from(number).ok())
            .map(Some)
            .ok_or_else(|| invalid(format!("supervision.{key} must be a non-negative integer")))
    }

    fn usize_of(value: u64, key: &str) -> Result<usize, CordisError> {
        usize::try_from(value).map_err(|_| invalid(format!("supervision.{key} is too large")))
    }

    impl SupervisionConfig {
        /// Read settings from a config node, typically the `supervision`
        /// key of a plugin's loader entry:
        ///
        /// ```yaml
        /// - id: zone-a
        ///   name: camera_zone_v2
        ///   config:
        ///     supervision:
        ///       strategy: one_for_one      # one_for_all | rest_for_one
        ///       max_restarts: 5
        ///       restart_window_secs: 10
        ///       backoff_initial_ms: 100
        ///       backoff_max_ms: 5000
        ///       shutdown_timeout_ms: 2000
        ///       worker_threads: 2
        ///       plugin_max_restarts: 3
        ///       plugin_restart_window_secs: 60
        /// ```
        ///
        /// A null node yields the defaults; missing keys keep theirs.
        ///
        /// # Errors
        ///
        /// `InvalidConfig` for a non-object node, an unknown key (to catch
        /// typos), a negative or non-integer number, an unknown strategy, or
        /// zero `worker_threads`.
        pub fn from_node(node: &Node) -> Result<Self, CordisError> {
            let mut config = Self::default();
            if node.is_null() {
                return Ok(config);
            }
            let Some(map) = node.as_object() else {
                return Err(invalid("supervision must be a mapping".to_owned()));
            };
            if let Some(unknown) = map.keys().find(|key| !KEYS.contains(&key.as_str())) {
                return Err(invalid(format!(
                    "unknown supervision key `{unknown}` (expected one of: {})",
                    KEYS.join(", ")
                )));
            }

            if let Some(strategy) = node["strategy"].as_str() {
                config.strategy = match strategy {
                    "one_for_one" => RestartStrategy::OneForOne,
                    "one_for_all" => RestartStrategy::OneForAll,
                    "rest_for_one" => RestartStrategy::RestForOne,
                    other => {
                        return Err(invalid(format!(
                            "unknown supervision.strategy `{other}` \
                             (expected one_for_one, one_for_all or rest_for_one)"
                        )));
                    }
                };
            } else if !node["strategy"].is_null() {
                return Err(invalid("supervision.strategy must be a string".to_owned()));
            }
            if let Some(value) = count(node, "max_restarts")? {
                config.max_restarts = usize_of(value, "max_restarts")?;
            }
            if let Some(value) = count(node, "restart_window_secs")? {
                config.restart_window = Duration::from_secs(value);
            }
            if let Some(value) = count(node, "backoff_initial_ms")? {
                config.backoff_initial = Duration::from_millis(value);
            }
            if let Some(value) = count(node, "backoff_max_ms")? {
                config.backoff_max = Duration::from_millis(value);
            }
            if let Some(value) = count(node, "shutdown_timeout_ms")? {
                config.shutdown_timeout = Duration::from_millis(value);
            }
            if let Some(value) = count(node, "worker_threads")? {
                config.worker_threads = usize_of(value, "worker_threads")?;
                if config.worker_threads == 0 {
                    return Err(invalid(
                        "supervision.worker_threads must be at least 1".to_owned(),
                    ));
                }
            }
            if let Some(value) = count(node, "plugin_max_restarts")? {
                config.plugin_max_restarts = usize_of(value, "plugin_max_restarts")?;
            }
            if let Some(value) = count(node, "plugin_restart_window_secs")? {
                config.plugin_restart_window = Duration::from_secs(value);
            }
            Ok(config)
        }
    }
}
