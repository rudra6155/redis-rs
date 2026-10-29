//! Process signals and a stop flag for background threads.

use std::io;
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::{Duration, Instant};

/// SIGINT + SIGTERM handlers.
///
/// [`ShutdownSignals::install`] registers them *eagerly*. `main` calls it before
/// printing the "Ready" line, so a signal sent the instant the server reports
/// readiness is already routed to graceful shutdown instead of hitting the
/// default action (which would kill the process with a non-zero status).
///
/// Registering a tokio handler replaces the default "terminate" action for the
/// rest of the process, which is why [`ShutdownSignals::force_exit_on_next`]
/// exists: without it a hung drain could not be interrupted with Ctrl-C.
pub struct ShutdownSignals {
    #[cfg(unix)]
    int: tokio::signal::unix::Signal,
    #[cfg(unix)]
    term: tokio::signal::unix::Signal,
}

impl ShutdownSignals {
    /// Register the handlers. Must be called from inside a tokio runtime.
    #[cfg(unix)]
    pub fn install() -> io::Result<ShutdownSignals> {
        use tokio::signal::unix::{signal, SignalKind};
        Ok(ShutdownSignals { int: signal(SignalKind::interrupt())?, term: signal(SignalKind::terminate())? })
    }

    /// Register the handlers (non-unix: Ctrl-C is handled lazily in `recv`).
    #[cfg(not(unix))]
    pub fn install() -> io::Result<ShutdownSignals> {
        Ok(ShutdownSignals {})
    }

    /// Wait for the next SIGINT/SIGTERM; returns its name.
    #[cfg(unix)]
    pub async fn recv(&mut self) -> &'static str {
        tokio::select! {
            _ = self.int.recv() => "SIGINT",
            _ = self.term.recv() => "SIGTERM",
        }
    }

    /// Wait for Ctrl-C.
    #[cfg(not(unix))]
    pub async fn recv(&mut self) -> &'static str {
        let _ = tokio::signal::ctrl_c().await;
        "CTRL-C"
    }

    /// After graceful shutdown has started, a second signal exits immediately (status 1).
    pub fn force_exit_on_next(mut self) {
        tokio::spawn(async move {
            let name = self.recv().await;
            eprintln!("received {name} again during shutdown, exiting immediately");
            std::process::exit(1);
        });
    }
}

/// A level-triggered stop flag that a background thread can sleep on:
/// `wait(d)` returns early (with `true`) as soon as someone calls `stop()`.
#[derive(Clone, Default)]
pub struct StopSignal {
    inner: Arc<(Mutex<bool>, Condvar)>,
}

impl StopSignal {
    /// New, not-yet-stopped flag.
    pub fn new() -> StopSignal {
        StopSignal::default()
    }

    /// Wake every waiter and make all future waits return `true` immediately.
    pub fn stop(&self) {
        let (flag, cv) = &*self.inner;
        *flag.lock().unwrap_or_else(PoisonError::into_inner) = true;
        cv.notify_all();
    }

    /// Has `stop()` been called?
    pub fn is_stopped(&self) -> bool {
        *self.inner.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Sleep for up to `dur`. Returns `true` if the flag is (or became) set.
    pub fn wait(&self, dur: Duration) -> bool {
        let (flag, cv) = &*self.inner;
        let deadline = Instant::now() + dur;
        let mut stopped = flag.lock().unwrap_or_else(PoisonError::into_inner);
        while !*stopped {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            stopped = cv.wait_timeout(stopped, deadline - now).unwrap_or_else(PoisonError::into_inner).0;
        }
        true
    }
}
