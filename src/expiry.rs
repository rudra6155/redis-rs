//! Time source and the background expiry sweeper.
//!
//! Expiry has two halves (SPEC.md section 5):
//!
//! * **lazy** - every `Shard` method checks the deadline of the key it touches
//!   (see `store.rs`); this file does not matter for correctness of reads.
//! * **active** - [`Sweeper`] wakes every [`SWEEP_INTERVAL`] and samples keys
//!   that have a TTL, so keys nobody reads again are eventually reclaimed.

use crate::signal::StopSignal;
use crate::store::Store;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Time between sweeper wake-ups (brief: 100 ms).
pub const SWEEP_INTERVAL: Duration = Duration::from_millis(100);
/// Keys sampled per shard per round while holding that shard's lock.
pub const SAMPLES_PER_ROUND: usize = 20;
/// Cap on consecutive rounds against one shard in one tick.
pub const MAX_ROUNDS_PER_SHARD: usize = 16;

/// Wall-clock milliseconds since the Unix epoch.
///
/// Deadlines are absolute wall-clock instants because they are persisted in
/// the AOF; a monotonic clock would be meaningless after a restart (Redis makes
/// the same choice).
pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

/// Handle to the background sweeper thread.
pub struct Sweeper {
    stop: StopSignal,
    join: Option<JoinHandle<()>>,
}

impl Sweeper {
    /// Start sweeping `store` every [`SWEEP_INTERVAL`].
    pub fn spawn(store: Arc<Store>) -> Sweeper {
        let stop = StopSignal::new();
        let thread_stop = stop.clone();
        let join = std::thread::Builder::new()
            .name("sweeper".into())
            .spawn(move || {
                while !thread_stop.wait(SWEEP_INTERVAL) {
                    store.sweep_cycle(now_ms());
                }
            })
            .expect("spawn sweeper thread");
        Sweeper { stop, join: Some(join) }
    }

    /// Stop the thread and wait for it to exit.
    pub fn stop(mut self) {
        self.shutdown();
    }

    fn shutdown(&mut self) {
        self.stop.stop();
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl Drop for Sweeper {
    fn drop(&mut self) {
        self.shutdown();
    }
}
