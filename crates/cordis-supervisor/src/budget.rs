//! Plugin-level restart budget, kept across fiber restarts.
//!
//! A fiber keeps its uid when Cordis restarts it, so escalations are
//! recorded per uid in a process-wide (per-binary, for dynamic plugins)
//! table.

use crate::SupervisionConfig;
use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

fn table() -> &'static Mutex<HashMap<u64, VecDeque<Instant>>> {
    static TABLE: OnceLock<Mutex<HashMap<u64, VecDeque<Instant>>>> = OnceLock::new();
    TABLE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn prune(times: &mut VecDeque<Instant>, window: Duration) {
    let now = Instant::now();
    while times
        .front()
        .is_some_and(|time| now.saturating_duration_since(*time) > window)
    {
        let _expired = times.pop_front();
    }
}

/// Records one escalation of fiber `uid`.
pub(crate) fn record_escalation(uid: u64) {
    let mut table = table()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    table.entry(uid).or_default().push_back(Instant::now());
}

/// Escalations of fiber `uid` within the configured window, if they exceed
/// the budget.
pub(crate) fn exhausted(uid: u64, config: &SupervisionConfig) -> Option<usize> {
    let mut table = table()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let times = table.get_mut(&uid)?;
    prune(times, config.plugin_restart_window);
    if times.is_empty() {
        let _removed = table.remove(&uid);
        return None;
    }
    (times.len() > config.plugin_max_restarts).then_some(times.len())
}

/// Forgets fiber `uid`'s escalations.
pub(crate) fn reset(uid: u64) {
    let mut table = table()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let _removed = table.remove(&uid);
}
