//! Fixture compiled to a `cdylib` by `tests/dynamic_swap.rs`: a plugin
//! whose supervised `decoder` task writes a heartbeat with the build tag
//! (`v1`, or `v2` under `--cfg zone_v2`) into the configured `dir`.
//!
//! The first run of the v1 build panics, to show that a panic inside a
//! dynamic library is caught (by the library's own std) and the task
//! restarted. Supervision, its runtime and the tasks all use the copies of
//! std and Tokio linked into this library.

use cordis_loader::Node;
use cordis_loader::dynamic::{BoxFuture, Config, Context, Plugin, PluginOutput, Result};
use cordis_supervisor::{PluginSupervisor, SupervisionConfig, Task, tokio};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[cfg(zone_v2)]
const VERSION: &str = "v2";
#[cfg(not(zone_v2))]
const VERSION: &str = "v1";

fn append(path: &Path, line: &str) {
    if let Ok(mut file) = std::fs::OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{line}");
    }
}

fn lines(path: &Path) -> usize {
    std::fs::read_to_string(path).map(|text| text.lines().count()).unwrap_or(0)
}

/// A camera-zone monitor.
pub struct CameraZone;

impl Plugin for CameraZone {
    fn name(&self) -> &str {
        "camera_zone"
    }

    fn apply(&self, ctx: Context, config: Config) -> BoxFuture<Result<PluginOutput>> {
        Box::pin(async move {
            let node = config.downcast::<Node>()?;
            let dir = PathBuf::from(
                node["dir"]
                    .as_str()
                    .ok_or_else(|| "camera_zone: missing `dir`".to_owned())?,
            );
            let supervision = SupervisionConfig::from_node(&node["supervision"])?;
            let supervisor = PluginSupervisor::start(&ctx, &supervision)?;

            let run_dir = dir.clone();
            let stop_dir = dir;
            supervisor.supervise(
                Task::new("decoder", move |signal| {
                    let dir = run_dir.clone();
                    async move {
                        let runs = dir.join(format!("runs-{VERSION}"));
                        append(&runs, "run");
                        if VERSION == "v1" && lines(&runs) == 1 {
                            panic!("decoder crashed on its first frame");
                        }
                        loop {
                            tokio::select! {
                                () = signal.cancelled() => return Ok(()),
                                () = tokio::time::sleep(Duration::from_millis(20)) => {
                                    std::fs::write(dir.join("heartbeat"), VERSION)?;
                                }
                            }
                        }
                    }
                })
                .on_stop(move || {
                    let dir = stop_dir.clone();
                    async move {
                        // Flushing takes a moment; the swap must wait for it.
                        tokio::time::sleep(Duration::from_millis(30)).await;
                        append(&dir.join(format!("stopped-{VERSION}")), "stopped");
                    }
                }),
            )?;
            Ok(PluginOutput::none())
        })
    }
}

cordis_loader::dynamic::export_plugin!(CameraZone);
