//! Append-only file.
//!
//! * Format: each write is a RESP array of bulk strings, exactly like Redis.
//!   The log records the *effect* of a command (absolute deadlines, expiry
//!   deletions), see `store.rs`.
//! * Writing: shards push records into an unbounded channel *while holding
//!   their shard lock* (so same-key order in the log equals apply order); one
//!   dedicated writer thread drains the channel in batches.
//! * Durability by policy ([`FsyncPolicy`]):
//!   `always` - the writer `fdatasync`s after each drained batch (group commit)
//!   and only then releases the connections waiting on a [`AofSink::barrier`];
//!   `everysec` - the writer only `write(2)`s; a separate thread `fdatasync`s
//!   once a second; `no` - `write(2)` only.
//! * Replay ([`replay`]): re-execute the log; an incomplete final command is
//!   truncated away (reported), any other corruption refuses to start.

use crate::command::{self, Ctx};
use crate::resp::{encode_request, parse_request, Frame};
use crate::signal::StopSignal;
use crate::store::Store;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::sync::oneshot;

/// Largest batch the writer assembles before issuing one `write(2)`.
const MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;
/// How often the `everysec` thread syncs.
const EVERYSEC_PERIOD: Duration = Duration::from_secs(1);

/// `--appendfsync`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// fsync before replying (group commit).
    Always,
    /// fsync from a background thread once a second.
    Everysec,
    /// never fsync (except at graceful shutdown).
    No,
}

impl FsyncPolicy {
    /// The flag spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            FsyncPolicy::Always => "always",
            FsyncPolicy::Everysec => "everysec",
            FsyncPolicy::No => "no",
        }
    }
}

impl FromStr for FsyncPolicy {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "always" => Ok(FsyncPolicy::Always),
            "everysec" => Ok(FsyncPolicy::Everysec),
            "no" => Ok(FsyncPolicy::No),
            other => Err(format!("invalid appendfsync '{other}' (expected always|everysec|no)")),
        }
    }
}

enum Msg {
    Record(Vec<u8>),
    Barrier(oneshot::Sender<()>),
    Shutdown(mpsc::Sender<()>),
}

/// Counters, mainly so tests can *observe* the fsync policy.
#[derive(Default, Debug)]
pub struct AofStats {
    /// Records written.
    pub records: AtomicU64,
    /// Bytes written.
    pub bytes: AtomicU64,
    /// `write(2)` calls.
    pub writes: AtomicU64,
    /// `fdatasync`/`fsync` calls.
    pub fsyncs: AtomicU64,
}

/// Cheap, cloneable producer handle held by every shard.
#[derive(Clone)]
pub struct AofSink {
    tx: mpsc::Sender<Msg>,
    closed: Arc<AtomicBool>,
}

impl AofSink {
    /// Queue one encoded record. Never blocks (unbounded channel), so it is safe
    /// to call under a shard lock: lock hold time never depends on the disk.
    pub fn record(&self, bytes: Vec<u8>) {
        if self.tx.send(Msg::Record(bytes)).is_err() && !self.closed.load(Ordering::Acquire) {
            die("AOF writer thread is gone; refusing to acknowledge writes that cannot be persisted");
        }
    }

    /// A receiver that resolves once every record queued *before* this call (by
    /// the calling thread) has been written and - under `always` - fsynced.
    pub fn barrier(&self) -> Option<oneshot::Receiver<()>> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Msg::Barrier(tx)).ok().map(|()| rx)
    }
}

fn die(msg: &str) -> ! {
    eprintln!("FATAL: {msg}");
    std::process::exit(1)
}

/// Owner of the writer (and `everysec` fsync) threads.
pub struct Aof {
    sink: AofSink,
    stats: Arc<AofStats>,
    policy: FsyncPolicy,
    writer: Option<JoinHandle<()>>,
    syncer: Option<JoinHandle<()>>,
    syncer_stop: StopSignal,
}

impl Aof {
    /// Open (creating if needed) `path` for appending and start the background threads.
    /// Call [`replay`] first: it may truncate the file.
    pub fn open(path: &Path, policy: FsyncPolicy) -> io::Result<Aof> {
        if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::create_dir_all(dir)?;
        }
        let existed = path.exists();
        let file = Arc::new(OpenOptions::new().create(true).append(true).open(path)?);
        if !existed {
            sync_parent_dir(path)?;
        }

        let (tx, rx) = mpsc::channel();
        let stats = Arc::new(AofStats::default());
        let dirty = Arc::new(AtomicBool::new(false));

        let writer = {
            let (file, dirty, stats) = (file.clone(), dirty.clone(), stats.clone());
            std::thread::Builder::new()
                .name("aof-writer".into())
                .spawn(move || writer_loop(&rx, &file, policy, &dirty, &stats))?
        };

        let syncer_stop = StopSignal::new();
        let syncer = if policy == FsyncPolicy::Everysec {
            let (file, stop, dirty, stats) = (file, syncer_stop.clone(), dirty, stats.clone());
            Some(std::thread::Builder::new().name("aof-fsync".into()).spawn(move || {
                while !stop.wait(EVERYSEC_PERIOD) {
                    if dirty.swap(false, Ordering::AcqRel) {
                        if let Err(e) = file.sync_data() {
                            die(&format!("AOF fsync failed: {e}"));
                        }
                        stats.fsyncs.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })?)
        } else {
            None
        };

        Ok(Aof {
            sink: AofSink { tx, closed: Arc::new(AtomicBool::new(false)) },
            stats,
            policy,
            writer: Some(writer),
            syncer,
            syncer_stop,
        })
    }

    /// Producer handle (hand to `Store::attach_aof`).
    pub fn sink(&self) -> AofSink {
        self.sink.clone()
    }

    /// The configured policy.
    pub fn policy(&self) -> FsyncPolicy {
        self.policy
    }

    /// Live counters.
    pub fn stats(&self) -> Arc<AofStats> {
        self.stats.clone()
    }

    /// Flush everything queued, `fsync`, and stop the threads. Idempotent.
    /// Callers must have stopped producing (connections drained, sweeper stopped).
    pub fn shutdown(&mut self) {
        let Some(writer) = self.writer.take() else {
            return;
        };
        self.sink.closed.store(true, Ordering::Release);
        let (done_tx, done_rx) = mpsc::channel();
        if self.sink.tx.send(Msg::Shutdown(done_tx)).is_ok() {
            let _ = done_rx.recv();
        }
        let _ = writer.join();
        self.syncer_stop.stop();
        if let Some(syncer) = self.syncer.take() {
            let _ = syncer.join();
        }
    }
}

impl Drop for Aof {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn writer_loop(rx: &mpsc::Receiver<Msg>, file: &File, policy: FsyncPolicy, dirty: &AtomicBool, stats: &AofStats) {
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    let mut barriers: Vec<oneshot::Sender<()>> = Vec::new();
    // `recv` fails only when every sender is gone; `Shutdown` is the normal exit.
    while let Ok(first) = rx.recv() {
        let mut shutdown: Option<mpsc::Sender<()>> = None;
        let mut records = 0u64;
        let mut next = Some(first);
        while let Some(msg) = next.take() {
            match msg {
                Msg::Record(bytes) => {
                    buf.extend_from_slice(&bytes);
                    records += 1;
                }
                Msg::Barrier(tx) => barriers.push(tx),
                Msg::Shutdown(done) => {
                    shutdown = Some(done);
                    break;
                }
            }
            if buf.len() < MAX_BATCH_BYTES {
                next = rx.try_recv().ok();
            }
        }

        if !buf.is_empty() {
            let mut f = file;
            if let Err(e) = f.write_all(&buf) {
                die(&format!("AOF write failed: {e}"));
            }
            stats.records.fetch_add(records, Ordering::Relaxed);
            stats.bytes.fetch_add(buf.len() as u64, Ordering::Relaxed);
            stats.writes.fetch_add(1, Ordering::Relaxed);
            buf.clear();
            match policy {
                FsyncPolicy::Always => {
                    if let Err(e) = file.sync_data() {
                        die(&format!("AOF fsync failed: {e}"));
                    }
                    stats.fsyncs.fetch_add(1, Ordering::Relaxed);
                }
                FsyncPolicy::Everysec | FsyncPolicy::No => dirty.store(true, Ordering::Release),
            }
        }
        // Group commit: everyone whose records were in this batch is released together,
        // strictly after the fsync above (under `always`).
        for tx in barriers.drain(..) {
            let _ = tx.send(());
        }

        if let Some(done) = shutdown {
            if let Err(e) = file.sync_all() {
                die(&format!("AOF final fsync failed: {e}"));
            }
            stats.fsyncs.fetch_add(1, Ordering::Relaxed);
            let _ = done.send(());
            return;
        }
    }
}

#[cfg(unix)]
fn sync_parent_dir(path: &Path) -> io::Result<()> {
    let dir = match path.parent() {
        Some(d) if !d.as_os_str().is_empty() => d,
        _ => Path::new("."),
    };
    File::open(dir)?.sync_all()
}

#[cfg(not(unix))]
fn sync_parent_dir(_path: &Path) -> io::Result<()> {
    Ok(())
}

// ---------------------------------------------------------------------------
// Replay
// ---------------------------------------------------------------------------

/// An incomplete final command that was cut off.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Truncation {
    /// Length the file was truncated to (end of the last complete command).
    pub last_good_offset: u64,
    /// Bytes removed.
    pub bytes_dropped: u64,
    /// First bytes of what was removed, as hex.
    pub offending_hex: String,
    /// Where the JSON report was written.
    pub report_path: PathBuf,
}

/// Result of a successful replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayOutcome {
    /// Commands re-executed.
    pub commands: u64,
    /// Bytes of valid log consumed.
    pub bytes: u64,
    /// Set if an incomplete tail was dropped.
    pub truncation: Option<Truncation>,
}

/// Why replay refused to continue.
#[derive(Debug)]
pub enum ReplayError {
    /// Could not read/truncate the file.
    Io(io::Error),
    /// The file is damaged somewhere other than a cut-off final command.
    Corrupt {
        /// Offset of the offending record.
        offset: u64,
        /// What was wrong.
        reason: String,
        /// First bytes of the offending record, as hex.
        offending_hex: String,
        /// Where the JSON report was written.
        report_path: PathBuf,
    },
}

impl fmt::Display for ReplayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReplayError::Io(e) => write!(f, "AOF I/O error: {e}"),
            ReplayError::Corrupt { offset, reason, offending_hex, report_path } => write!(
                f,
                "AOF is corrupt at byte offset {offset}: {reason} (offending bytes: {offending_hex}); \
                 refusing to start, see {}",
                report_path.display()
            ),
        }
    }
}

impl std::error::Error for ReplayError {}

impl From<io::Error> for ReplayError {
    fn from(e: io::Error) -> Self {
        ReplayError::Io(e)
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().take(32).map(|b| format!("{b:02x}")).collect::<Vec<_>>().join(" ")
}

fn report_path_for(aof: &Path) -> PathBuf {
    let name = aof.file_name().map_or_else(|| "aof".into(), |n| n.to_string_lossy().into_owned());
    aof.with_file_name(format!("{name}.corruption-report.txt"))
}

fn json_escape(s: &str) -> String {
    s.chars()
        .flat_map(|c| match c {
            '"' => vec!['\\', '"'],
            '\\' => vec!['\\', '\\'],
            '\n' => vec!['\\', 'n'],
            c => vec![c],
        })
        .collect()
}

fn write_report(
    aof: &Path,
    action: &str,
    bytes_dropped: u64,
    last_good_offset: u64,
    offending_hex: &str,
    reason: &str,
) -> PathBuf {
    let report_path = report_path_for(aof);
    let body = format!(
        "{{\n  \"action\": \"{action}\",\n  \"aof_path\": \"{}\",\n  \"bytes_dropped\": {bytes_dropped},\n  \
         \"last_good_offset\": {last_good_offset},\n  \"offending_hex\": \"{offending_hex}\",\n  \"reason\": \"{}\"\n}}\n",
        json_escape(&aof.display().to_string()),
        json_escape(reason),
    );
    if let Err(e) = std::fs::write(&report_path, body) {
        eprintln!("warning: could not write {}: {e}", report_path.display());
    }
    report_path
}

/// Re-execute the AOF at `path` against `store` (which must not have an AOF attached).
///
/// Missing file = empty database. See the module docs for the corruption policy.
pub fn replay(path: &Path, store: &Store) -> Result<ReplayOutcome, ReplayError> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Ok(ReplayOutcome { commands: 0, bytes: 0, truncation: None });
        }
        Err(e) => return Err(e.into()),
    };

    let corrupt = |offset: u64, reason: String, bytes: &[u8]| -> ReplayError {
        let offending_hex = hex(bytes);
        let report_path = write_report(path, "refused_to_start", 0, offset, &offending_hex, &reason);
        ReplayError::Corrupt { offset, reason, offending_hex, report_path }
    };

    let mut ctx = Ctx::replay(store);
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = vec![0u8; 1 << 20];
    let mut offset: u64 = 0; // file offset of buf[0]
    let mut commands: u64 = 0;

    loop {
        let mut pos = 0;
        loop {
            match parse_request(&buf[pos..]) {
                Ok(Some((args, used))) => {
                    if !args.is_empty() {
                        if let Frame::Error(e) = command::execute(&mut ctx, &args) {
                            return Err(corrupt(
                                offset + pos as u64,
                                format!("record failed to re-execute: {}", String::from_utf8_lossy(&e)),
                                &buf[pos..pos + used],
                            ));
                        }
                        commands += 1;
                    }
                    pos += used;
                }
                Ok(None) => break,
                Err(e) => {
                    return Err(corrupt(offset + pos as u64, format!("invalid RESP: {e}"), &buf[pos..]));
                }
            }
        }
        buf.drain(..pos);
        offset += pos as u64;

        let n = file.read(&mut chunk)?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
    }

    if buf.is_empty() {
        return Ok(ReplayOutcome { commands, bytes: offset, truncation: None });
    }

    // EOF in the middle of a command: the torn write of a crash. Drop it, loudly.
    let offending_hex = hex(&buf);
    let bytes_dropped = buf.len() as u64;
    let reason = "file ends in the middle of a command (torn write)";
    let report_path = write_report(path, "truncated_tail", bytes_dropped, offset, &offending_hex, reason);
    eprintln!(
        "AOF: dropped {bytes_dropped} bytes of incomplete final command at offset {offset} \
         (first bytes: {offending_hex}); report: {}",
        report_path.display()
    );
    let f = OpenOptions::new().write(true).open(path)?;
    f.set_len(offset)?;
    f.sync_all()?;
    Ok(ReplayOutcome {
        commands,
        bytes: offset,
        truncation: Some(Truncation { last_good_offset: offset, bytes_dropped, offending_hex, report_path }),
    })
}

/// Encode a record the same way the shards do (used by tests that craft AOF files).
pub fn encode_record(args: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    encode_request(args, &mut out);
    out
}
