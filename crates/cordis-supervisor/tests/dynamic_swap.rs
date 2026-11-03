//! Dynamic-library plugins with supervised tasks, and swapping one plugin's
//! code for a new build inside a running process.
//!
//! `tests/fixtures/camera_zone.rs` is compiled twice into `cdylib`s
//! (`camera_zone_v1`, and `camera_zone_v2` under `--cfg zone_v2`) with the
//! same toolchain and workspace rlibs as this test, then loaded through a
//! real [`Loader`].

// The fixture reads its supervision settings with `SupervisionConfig::from_node`.
#![cfg(feature = "node")]

use cordis::{Inject, PluginOutput, plugin_sync};
use cordis_loader::{Loader, LoaderConfig, PluginRegistry};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime};

fn temp_dir(stem: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "cordis-supervisor-test-{stem}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The workspace `target/` directory holding the built rlibs.
fn target_dir() -> PathBuf {
    std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .ancestors()
                .nth(2)
                .unwrap()
                .join("target")
        })
}

/// The rustc that built this test binary.
fn rustc() -> PathBuf {
    if let Some(rustc) = std::env::var_os("RUSTC") {
        let rustc = PathBuf::from(rustc);
        if rustc.is_file() {
            return rustc;
        }
    }
    let sibling = PathBuf::from(env!("CARGO")).parent().unwrap().join("rustc");
    if sibling.is_file() {
        return sibling;
    }
    PathBuf::from("rustc")
}

/// The freshest built rlib for a workspace crate.
fn find_rlib(name: &str) -> PathBuf {
    let debug = target_dir().join("debug");
    let prefix = format!("lib{name}-");
    let mut candidates: Vec<(SystemTime, PathBuf)> = Vec::new();
    let uplifted = debug.join(format!("lib{name}.rlib"));
    if let Ok(metadata) = std::fs::metadata(&uplifted) {
        candidates.push((metadata.modified().unwrap(), uplifted));
    }
    if let Ok(entries) = std::fs::read_dir(debug.join("deps")) {
        for entry in entries.flatten() {
            let path = entry.path();
            let is_match = path
                .extension()
                .is_some_and(|extension| extension == "rlib")
                && path
                    .file_name()
                    .and_then(|file| file.to_str())
                    .is_some_and(|file| file.starts_with(&prefix));
            if is_match {
                candidates.push((entry.metadata().unwrap().modified().unwrap(), path));
            }
        }
    }
    candidates.sort_by_key(|candidate| candidate.0);
    candidates
        .pop()
        .unwrap_or_else(|| panic!("no rlib found for {name}"))
        .1
}

fn cdylib_file_name(crate_name: &str) -> String {
    if cfg!(target_os = "windows") {
        format!("{crate_name}.dll")
    } else if cfg!(target_os = "macos") {
        format!("lib{crate_name}.dylib")
    } else {
        format!("lib{crate_name}.so")
    }
}

/// Build-script output directories, for native libraries a plain rustc
/// invocation would otherwise not find.
fn native_search_dirs() -> Vec<PathBuf> {
    std::fs::read_dir(target_dir().join("debug/build"))
        .map(|entries| {
            entries
                .flatten()
                .map(|entry| entry.path().join("out"))
                .filter(|out| out.is_dir())
                .collect()
        })
        .unwrap_or_default()
}

/// Compile the camera-zone fixture as crate `crate_name`.
fn compile_zone(out_dir: &Path, crate_name: &str, cfgs: &[&str]) -> PathBuf {
    std::fs::create_dir_all(out_dir).unwrap();
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/camera_zone.rs");
    let mut command = Command::new(rustc());
    command
        .args(["--edition", "2024", "--crate-type", "cdylib"])
        .arg(format!("--crate-name={crate_name}"))
        .arg("--out-dir")
        .arg(out_dir)
        .arg(&source)
        .arg("-L")
        .arg(format!(
            "dependency={}",
            target_dir().join("debug/deps").display()
        ))
        .arg("--extern")
        .arg(format!(
            "cordis_loader={}",
            find_rlib("cordis_loader").display()
        ))
        .arg("--extern")
        .arg(format!(
            "cordis_supervisor={}",
            find_rlib("cordis_supervisor").display()
        ));
    for dir in native_search_dirs() {
        command.arg("-L").arg(format!("native={}", dir.display()));
    }
    for cfg in cfgs {
        command.arg(format!("--cfg={cfg}"));
    }
    let output = command.output().expect("run rustc");
    assert!(
        output.status.success(),
        "compiling {crate_name} failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    out_dir.join(cdylib_file_name(crate_name))
}

fn json_path(path: &Path) -> String {
    path.display().to_string().replace('\\', "/")
}

fn entries(zone_name: &str, data: &Path) -> String {
    format!(
        r#"{{"entries":[
            {{"id":"zone","name":"{zone_name}","config":{{"dir":"{}","supervision":{{
                "backoff_initial_ms":5,"backoff_max_ms":20,"worker_threads":1}}}}}},
            {{"id":"steady","name":"steady"}}
        ]}}"#,
        json_path(data)
    )
}

fn lines(path: &Path) -> usize {
    std::fs::read_to_string(path)
        .map(|text| text.lines().count())
        .unwrap_or(0)
}

fn wait_for(what: &str, check: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if check() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("timed out waiting for {what}");
}

#[test]
fn dynamic_plugin_is_supervised_and_swapped_without_a_restart() {
    std::panic::set_hook(Box::new(|_| {}));
    let dir = temp_dir("swap");
    let plugins = dir.join("plugins");
    let data = dir.join("data");
    std::fs::create_dir_all(&plugins).unwrap();
    std::fs::create_dir_all(&data).unwrap();
    let build = dir.join("build");
    let v1 = compile_zone(&build, "camera_zone_v1", &[]);
    let v2 = compile_zone(&build, "camera_zone_v2", &["zone_v2"]);
    std::fs::copy(&v1, plugins.join(cdylib_file_name("camera_zone_v1"))).unwrap();

    // A statically registered plugin running alongside, to prove the swap
    // leaves other plugins alone.
    let applies = Arc::new(AtomicUsize::new(0));
    let unloads = Arc::new(AtomicUsize::new(0));
    let mut registry = PluginRegistry::new().with_dynamic_dirs([&plugins]);
    let (plugin_applies, plugin_unloads) = (Arc::clone(&applies), Arc::clone(&unloads));
    registry.register("steady", move || {
        let (applies, unloads) = (Arc::clone(&plugin_applies), Arc::clone(&plugin_unloads));
        plugin_sync::<cordis_include::Node, _>("steady", Inject::default(), move |_ctx, _| {
            applies.fetch_add(1, Ordering::SeqCst);
            let unloads = Arc::clone(&unloads);
            Ok(PluginOutput::infallible(move || {
                unloads.fetch_add(1, Ordering::SeqCst);
            }))
        })
    });

    let config = dir.join("cordis.json");
    std::fs::write(&config, entries("camera_zone_v1", &data)).unwrap();
    let root = cordis::Context::new();
    let loader = Loader::open(&root, LoaderConfig::new(&config).with_registry(registry)).unwrap();
    assert!(loader.last_error().is_none(), "{:?}", loader.last_error());

    // v1 runs from the library, panics once inside it, and is restarted by
    // its own supervisor; Tokio timers inside the library work.
    wait_for("v1 to restart after its panic", || {
        lines(&data.join("runs-v1")) == 2
    });
    wait_for("a v1 heartbeat", || {
        std::fs::read_to_string(data.join("heartbeat"))
            .ok()
            .as_deref()
            == Some("v1")
    });
    assert_eq!(
        lines(&data.join("stopped-v1")),
        1,
        "the crashed run's hook ran"
    );

    // Swap: drop in the new build under a new name and point the entry at it.
    std::fs::copy(&v2, plugins.join(cdylib_file_name("camera_zone_v2"))).unwrap();
    std::fs::write(&config, entries("camera_zone_v2", &data)).unwrap();
    loader.reload().unwrap();
    assert!(loader.last_error().is_none(), "{:?}", loader.last_error());

    // The old build's task was stopped and its hook finished during the
    // reload; the new build took over.
    assert_eq!(
        lines(&data.join("stopped-v1")),
        2,
        "v1's final stop hook ran"
    );
    wait_for("a v2 heartbeat", || {
        std::fs::read_to_string(data.join("heartbeat"))
            .ok()
            .as_deref()
            == Some("v2")
    });
    assert_eq!(lines(&data.join("runs-v2")), 1);

    // Nothing else was restarted.
    assert_eq!(applies.load(Ordering::SeqCst), 1);
    assert_eq!(unloads.load(Ordering::SeqCst), 0);

    // Stopping the loader stops v2 cleanly too.
    loader.dispose().unwrap();
    assert_eq!(lines(&data.join("stopped-v2")), 1);

    let _ = std::fs::remove_dir_all(&dir);
}
