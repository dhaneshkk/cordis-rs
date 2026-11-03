//! Worker runtime: boot the loader, watch the config, exit on signals.

use cordis::Context;
use cordis_loader::dynamic::DynamicPluginResolver;
use cordis_loader::{Loader, LoaderConfig, PluginRegistry};
use notify::{Config as NotifyConfig, EventKind, RecursiveMode, Watcher};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// Exit code asking the daemon for a hot restart.
pub const EXIT_RESTART: i32 = 51;
/// Exit code telling the daemon to quit without restarting.
pub const EXIT_QUIT: i32 = 52;
/// Exit code reporting that the loader never came up (bad config,
/// unreadable file); the daemon exits non-zero instead of masking it.
pub const EXIT_BOOT: i32 = 53;
/// Environment marker set by the daemon on workers it supervises. Such
/// workers watch their stdin pipe: the daemon holds the write end, so EOF
/// means the daemon is going away (clean shutdown or sudden death) and the
/// worker tears itself down gracefully.
pub const SUPERVISED_ENV: &str = "CORDIS_SUPERVISED";

/// Quiet window a plugin library must stay unchanged before the worker
/// restarts, so incremental linker output does not restart mid-write.
const PLUGIN_CHANGE_DEBOUNCE: Duration = Duration::from_millis(250);

/// Handle exposed as the `worker` service so plugins can stop or restart
/// the process (upstream's `ctx.loader.exit` / full-reload protocol).
pub struct WorkerHandle {
    inner: Arc<WorkerInner>,
}

impl WorkerHandle {
    /// Hot restart: dispose everything and ask the daemon for a new worker.
    pub fn restart(&self) -> ! {
        self.inner.teardown();
        std::process::exit(EXIT_RESTART);
    }

    /// Quit: dispose everything and tell the daemon not to restart.
    pub fn shutdown(&self) -> ! {
        self.inner.teardown();
        std::process::exit(EXIT_QUIT);
    }
}

/// Everything the worker owns; shared with the signal handler.
struct WorkerInner {
    root: Context,
    loader: Option<Loader>,
}

impl WorkerInner {
    fn teardown(&self) {
        if let Some(loader) = &self.loader {
            let _ = loader.dispose();
        }
        let _ = self.root.fiber().and_then(|fiber| fiber.dispose());
    }
}

/// Run the worker process: load dotenv, boot the loader (with dynamic
/// plugin directories, if any), watch the entry file and the plugin
/// directories, and block until a signal (or a `worker` service call)
/// exits the process. Never returns.
///
/// With `plugin_swap`, a library change only restarts the worker when that
/// library is already loaded; see [`crate::Options::plugin_swap`].
pub fn run(config_path: &Path, plugin_dirs: &[PathBuf], plugin_swap: bool) -> ! {
    let root = Context::new();
    let mut registry = PluginRegistry::new();
    let resolver = DynamicPluginResolver::new(plugin_dirs.iter());
    if !plugin_dirs.is_empty() {
        // Clones share the loaded-library cache, so the watcher below sees
        // what the loader has opened.
        registry.set_dynamic(resolver.clone());
    }
    let loader = match Loader::open(
        &root,
        LoaderConfig::new(config_path).with_registry(registry),
    ) {
        Ok(loader) => loader,
        Err(error) => {
            eprintln!(
                "cordis: failed to start from {}: {error}",
                config_path.display()
            );
            std::process::exit(EXIT_BOOT);
        }
    };
    let inner = Arc::new(WorkerInner {
        root: root.clone(),
        loader: Some(loader.clone()),
    });

    let handle = Arc::new(WorkerHandle {
        inner: Arc::clone(&inner),
    });
    if let Err(error) = root.provide_arc("worker", handle.clone()) {
        eprintln!("cordis: could not expose the worker service: {error}");
    }

    let signal_inner = Arc::clone(&inner);
    if ctrlc::set_handler(move || {
        eprintln!("cordis: signal received, shutting down");
        signal_inner.teardown();
        std::process::exit(EXIT_QUIT);
    })
    .is_err()
    {
        eprintln!("cordis: could not install signal handlers");
    }

    // Under the daemon, watch the supervisor's stdin pipe. EOF (or an
    // errored pipe) means the daemon is gone or asked us to stop — the
    // same graceful teardown a signal triggers, and the only notification
    // a SIGKILLed daemon can still send. Not spawned for manually-run
    // workers, so a terminal's stdin stays untouched.
    if std::env::var_os(SUPERVISED_ENV).is_some() {
        let inner = Arc::clone(&inner);
        let watched = std::thread::Builder::new()
            .name("cordis-supervisor-watch".to_owned())
            .spawn(move || {
                let mut stdin = std::io::stdin();
                let mut byte = [0_u8];
                loop {
                    match stdin.read(&mut byte) {
                        Ok(0) | Err(_) => break,
                        Ok(_) => continue,
                    }
                }
                eprintln!("cordis: supervisor went away, shutting down");
                inner.teardown();
                std::process::exit(EXIT_QUIT);
            });
        if watched.is_err() {
            eprintln!("cordis: could not watch the supervisor pipe");
        }
    }

    // Dropping the watcher stops it, so it must live as long as the worker
    // (this function never returns).
    let _config_watcher = match loader.watch() {
        Ok(watcher) => Some(watcher),
        Err(error) => {
            eprintln!(
                "cordis: config hot reload disabled ({error}); restart manually to apply changes"
            );
            None
        }
    };

    if !plugin_dirs.is_empty() {
        // A changed library only takes effect through a fresh worker: the
        // old process never unloads a mapped library, so the watcher asks
        // the daemon for a restart instead of reloading in place. In swap
        // mode only loaded libraries count; new files wait for the config.
        let swap = plugin_swap.then_some(resolver);
        let mode = if swap.is_some() {
            "changes to loaded libraries hot-restart the worker; \
             new libraries are swapped in through the config"
        } else {
            "library changes hot-restart the worker"
        };
        match watch_plugin_dirs(plugin_dirs, handle, swap) {
            Ok(()) => eprintln!(
                "cordis: dynamic plugins from {} ({mode})",
                plugin_dirs
                    .iter()
                    .map(|dir| dir.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Err(error) => eprintln!(
                "cordis: plugin library watching disabled ({error}); restart manually to apply changes"
            ),
        }
    }

    if let Some(error) = loader.last_error() {
        eprintln!("cordis: startup issue: {error}");
    }

    eprintln!(
        "cordis: worker ready ({} entries, config: {})",
        loader.tree().entries().len(),
        config_path.display()
    );

    // The worker's work happens on fiber threads and watcher callbacks;
    // park until a signal or service call ends the process.
    loop {
        std::thread::park();
    }
}

/// Watch `dirs` (non-recursively) for plugin library changes and hot
/// restart the worker through `handle` once a change settles.
///
/// Only files with a dynamic-library extension count; matching by
/// extension (instead of exact paths) also absorbs macOS FSEvents
/// reporting realpaths. With `swap`, a change only counts when its file
/// name matches a library `swap` has already loaded. The thread ends the
/// process on the first settled change, so there is nothing to return.
fn watch_plugin_dirs(
    dirs: &[PathBuf],
    handle: Arc<WorkerHandle>,
    swap: Option<DynamicPluginResolver>,
) -> Result<(), String> {
    let (tx, rx) = mpsc::channel();
    let mut watcher = notify::RecommendedWatcher::new(tx, NotifyConfig::default())
        .map_err(|error| format!("cannot create watcher: {error}"))?;
    for dir in dirs {
        watcher
            .watch(dir, RecursiveMode::NonRecursive)
            .map_err(|error| format!("cannot watch {}: {error}", dir.display()))?;
    }

    std::thread::Builder::new()
        .name("cordis-plugin-watch".to_owned())
        .spawn(move || {
            let _watcher = watcher; // keep the watch alive for the thread's lifetime
            // Settle on changes to relevant libraries only: unrelated
            // activity in the directory must not postpone the restart.
            let mut last_change: Option<Instant> = None;
            loop {
                let wait = last_change.map_or(PLUGIN_CHANGE_DEBOUNCE, |at| {
                    PLUGIN_CHANGE_DEBOUNCE
                        .saturating_sub(at.elapsed())
                        .max(Duration::from_millis(1))
                });
                match rx.recv_timeout(wait) {
                    // Opening or reading a library (the loader's own
                    // dlopen included) changes nothing; only writes,
                    // creations, renames and removals count.
                    Ok(Ok(event)) if matches!(event.kind, EventKind::Access(_)) => {}
                    Ok(Ok(event)) => {
                        let relevant = event.paths.iter().any(|path| {
                            is_plugin_library(path)
                                && swap.as_ref().is_none_or(|resolver| {
                                    is_loaded_library(path, &resolver.loaded_paths())
                                })
                        });
                        if relevant {
                            last_change = Some(Instant::now());
                        }
                    }
                    Ok(Err(_)) | Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
                if last_change.is_some_and(|at| at.elapsed() >= PLUGIN_CHANGE_DEBOUNCE) {
                    eprintln!("cordis: plugin library changed, restarting worker");
                    handle.restart();
                }
            }
        })
        .map_err(|error| format!("cannot spawn watcher thread: {error}"))?;
    Ok(())
}

/// Whether `path` names one of the `loaded` libraries. Compared by file
/// name: the watched directories are the ones libraries are loaded from,
/// and a replaced or removed file can no longer be canonicalized.
fn is_loaded_library(path: &Path, loaded: &[PathBuf]) -> bool {
    path.file_name()
        .is_some_and(|name| loaded.iter().any(|loaded| loaded.file_name() == Some(name)))
}

/// Whether `path` looks like a dynamic library by extension.
fn is_plugin_library(path: &Path) -> bool {
    path.extension().is_some_and(|extension| {
        matches!(
            extension.to_ascii_lowercase().to_str(),
            Some("so" | "dylib" | "dll")
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dynamic_library_extensions_match() {
        assert!(is_plugin_library(Path::new("libgreeter.so")));
        assert!(is_plugin_library(Path::new("libgreeter.dylib")));
        assert!(is_plugin_library(Path::new("greeter.dll")));
        assert!(is_plugin_library(Path::new("GREETER.SO")));
        assert!(!is_plugin_library(Path::new("cordis.yml")));
        assert!(!is_plugin_library(Path::new("libgreeter.so.tmp")));
        assert!(!is_plugin_library(Path::new("plugins")));
    }

    #[test]
    fn loaded_libraries_match_by_file_name() {
        let loaded = [PathBuf::from("/srv/plugins/libzone_v1.so")];
        assert!(is_loaded_library(
            Path::new("/private/srv/plugins/libzone_v1.so"),
            &loaded
        ));
        assert!(!is_loaded_library(
            Path::new("/srv/plugins/libzone_v2.so"),
            &loaded
        ));
        assert!(!is_loaded_library(
            Path::new("/srv/plugins/libzone_v2.so"),
            &[]
        ));
    }
}
