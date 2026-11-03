//! Supervision of statically registered plugins: task restarts, unload
//! cleanup, isolation between plugins, escalation to the fiber, and
//! per-plugin config reload.

use cordis::{Context, FiberState, Inject, PluginOutput, plugin_sync};
use cordis_supervisor::{PluginSupervisor, RestartPolicy, SupervisionConfig, Task, tokio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// What every task in a test reports.
#[derive(Default)]
struct Log {
    events: Mutex<Vec<String>>,
}

impl Log {
    fn push(&self, event: impl Into<String>) {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(event.into());
    }

    fn count(&self, event: &str) -> usize {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|logged| *logged == event)
            .count()
    }
}

fn fast() -> SupervisionConfig {
    SupervisionConfig {
        backoff_initial: Duration::from_millis(5),
        backoff_max: Duration::from_millis(20),
        shutdown_timeout: Duration::from_secs(2),
        worker_threads: 1,
        ..SupervisionConfig::default()
    }
}

fn wait_until(what: &str, check: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if check() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("timed out waiting for {what}");
}

fn quiet_panics() {
    std::panic::set_hook(Box::new(|_| {}));
}

/// A task that logs `run:<id>`, panics on its first `fail_first` runs, then
/// idles until stopped; its stop hook logs `stop:<id>` after a short flush.
fn task(id: &str, log: &Arc<Log>, fail_first: usize) -> Task {
    let runs = Arc::new(AtomicUsize::new(0));
    let (run_log, stop_log) = (Arc::clone(log), Arc::clone(log));
    let (run_id, stop_id) = (id.to_owned(), id.to_owned());
    Task::new(id, move |signal| {
        let (log, id, runs) = (Arc::clone(&run_log), run_id.clone(), Arc::clone(&runs));
        async move {
            log.push(format!("run:{id}"));
            if runs.fetch_add(1, Ordering::SeqCst) < fail_first {
                panic!("task {id} crashed");
            }
            // Tokio timers work inside tasks: the plugin's own runtime.
            tokio::time::sleep(Duration::from_millis(1)).await;
            signal.cancelled().await;
            Ok(())
        }
    })
    .on_stop(move || {
        let (log, id) = (Arc::clone(&stop_log), stop_id.clone());
        async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            log.push(format!("stop:{id}"));
        }
    })
}

#[test]
fn panicking_task_restarts_and_unload_waits_for_cleanup() {
    quiet_panics();
    let log = Arc::new(Log::default());
    let root = Context::new();
    let plugin_log = Arc::clone(&log);
    let fiber = root.plugin_default(plugin_sync::<(), _>(
        "zone",
        Inject::default(),
        move |ctx, _| {
            let supervisor = PluginSupervisor::start(&ctx, &fast())?;
            supervisor.supervise(task("decoder", &plugin_log, 2))?;
            Ok(PluginOutput::none())
        },
    ));
    fiber.try_wait().unwrap();

    wait_until("two panics and a restart", || log.count("run:decoder") == 3);
    assert_eq!(fiber.state(), FiberState::Active);

    fiber.dispose().unwrap();
    // Two crashed runs and the final stop each ran the hook, and all of it
    // finished before dispose returned.
    assert_eq!(log.count("stop:decoder"), 3);
}

#[test]
fn one_plugins_crash_loop_does_not_touch_another_plugin() {
    quiet_panics();
    let log = Arc::new(Log::default());
    let root = Context::new();

    let bad_log = Arc::clone(&log);
    let bad = root.plugin_default(plugin_sync::<(), _>(
        "bad",
        Inject::default(),
        move |ctx, _| {
            let config = SupervisionConfig {
                max_restarts: 2,
                plugin_max_restarts: 1,
                ..fast()
            };
            let supervisor = PluginSupervisor::start(&ctx, &config)?;
            supervisor.supervise(task("crasher", &bad_log, usize::MAX))?;
            Ok(PluginOutput::none())
        },
    ));
    let good_log = Arc::clone(&log);
    let good = root.plugin_default(plugin_sync::<(), _>(
        "good",
        Inject::default(),
        move |ctx, _| {
            let supervisor = PluginSupervisor::start(&ctx, &fast())?;
            supervisor.supervise(task("steady", &good_log, 0))?;
            Ok(PluginOutput::none())
        },
    ));
    good.try_wait().unwrap();

    // The bad plugin escalates (fiber restart), escalates again, and is
    // left Failed once its plugin-level budget of 1 is used up.
    wait_until("the bad plugin to fail", || {
        bad.state() == FiberState::Failed
    });
    let error = bad
        .error()
        .map(|error| error.to_string())
        .unwrap_or_default();
    assert!(
        error.contains("escalated 2 times"),
        "unexpected error: {error}"
    );
    assert_eq!(
        log.count("run:crasher"),
        6,
        "3 runs (initial + 2 restarts) per supervisor, for the original and one restart"
    );

    assert_eq!(good.state(), FiberState::Active);
    assert_eq!(
        log.count("run:steady"),
        1,
        "the healthy plugin was never restarted"
    );
    assert_eq!(log.count("stop:steady"), 0);

    // An operator can clear the budget and retry the failed plugin.
    PluginSupervisor::reset_restart_budget(&bad);
    good.dispose().unwrap();
    let _ = bad.dispose();
}

#[derive(Clone)]
struct ZoneConfig {
    fps: u32,
}

#[test]
fn reconfiguring_one_plugin_replaces_only_its_supervisor() {
    let log = Arc::new(Log::default());
    let root = Context::new();
    let zone_log = Arc::clone(&log);
    let zone = root.plugin(
        plugin_sync::<ZoneConfig, _>("zone", Inject::default(), move |ctx, config| {
            let supervisor = PluginSupervisor::start(&ctx, &fast())?;
            supervisor.supervise(task(&format!("decoder@{}fps", config.fps), &zone_log, 0))?;
            Ok(PluginOutput::none())
        }),
        ZoneConfig { fps: 15 },
    );
    let other_log = Arc::clone(&log);
    let other = root.plugin_default(plugin_sync::<(), _>(
        "other",
        Inject::default(),
        move |ctx, _| {
            let supervisor = PluginSupervisor::start(&ctx, &fast())?;
            supervisor.supervise(task("other", &other_log, 0))?;
            Ok(PluginOutput::none())
        },
    ));
    zone.try_wait().unwrap();
    other.try_wait().unwrap();
    wait_until("both plugins running", || {
        log.count("run:decoder@15fps") == 1 && log.count("run:other") == 1
    });

    zone.update(ZoneConfig { fps: 30 }).unwrap();

    assert_eq!(
        log.count("stop:decoder@15fps"),
        1,
        "old tasks stopped before the reload"
    );
    wait_until("the reconfigured task", || {
        log.count("run:decoder@30fps") == 1
    });
    assert_eq!(log.count("run:other"), 1, "the other plugin kept running");
    assert_eq!(log.count("stop:other"), 0);

    zone.dispose().unwrap();
    other.dispose().unwrap();
}

#[test]
fn task_management() {
    let log = Arc::new(Log::default());
    let root = Context::new();
    let slot: Arc<Mutex<Option<PluginSupervisor>>> = Arc::default();
    let plugin_slot = Arc::clone(&slot);
    let plugin_log = Arc::clone(&log);
    let fiber = root.plugin_default(plugin_sync::<(), _>(
        "zone",
        Inject::default(),
        move |ctx, _| {
            let supervisor = PluginSupervisor::start(&ctx, &fast())?;
            supervisor.supervise(task("a", &plugin_log, 0))?;
            supervisor.supervise(task("b", &plugin_log, 0).policy(RestartPolicy::Transient))?;
            *plugin_slot.lock().unwrap() = Some(supervisor);
            Ok(PluginOutput::none())
        },
    ));
    fiber.try_wait().unwrap();
    let supervisor = slot.lock().unwrap().clone().unwrap();

    assert_eq!(supervisor.name(), "zone");
    assert_eq!(supervisor.tasks().unwrap(), vec!["a", "b"]);
    assert!(
        supervisor.supervise(task("a", &log, 0)).is_err(),
        "task ids are unique per plugin"
    );
    supervisor.stop_task("a").unwrap();
    assert_eq!(log.count("stop:a"), 1);
    assert_eq!(supervisor.tasks().unwrap(), vec!["b"]);

    fiber.dispose().unwrap();
    assert!(
        supervisor.tasks().is_err(),
        "the supervisor stops with its plugin"
    );
}

#[cfg(feature = "node")]
mod node_config {
    use cordis_include::Node;
    use cordis_supervisor::{RestartStrategy, SupervisionConfig};
    use std::time::Duration;

    fn node(yaml: &str) -> Node {
        cordis_include::yaml::parse_node(yaml).expect("valid yaml")
    }

    #[test]
    fn reads_settings_and_keeps_defaults() {
        assert_eq!(
            SupervisionConfig::from_node(&Node::Null).unwrap(),
            SupervisionConfig::default()
        );
        let config = SupervisionConfig::from_node(&node(
            "strategy: one_for_all\nmax_restarts: 7\nbackoff_max_ms: 250\nworker_threads: 4\n",
        ))
        .unwrap();
        assert_eq!(config.strategy, RestartStrategy::OneForAll);
        assert_eq!(config.max_restarts, 7);
        assert_eq!(config.backoff_max, Duration::from_millis(250));
        assert_eq!(config.worker_threads, 4);
        assert_eq!(
            config.shutdown_timeout,
            SupervisionConfig::default().shutdown_timeout
        );
    }

    #[test]
    fn rejects_mistakes() {
        for (yaml, expected) in [
            ("max_restart: 3\n", "unknown supervision key `max_restart`"),
            ("strategy: one_for_some\n", "unknown supervision.strategy"),
            ("max_restarts: -1\n", "non-negative integer"),
            ("worker_threads: 0\n", "at least 1"),
            ("strategy: 3\n", "must be a string"),
        ] {
            let error = SupervisionConfig::from_node(&node(yaml))
                .unwrap_err()
                .to_string();
            assert!(error.contains(expected), "{yaml:?}: {error}");
        }
    }
}
