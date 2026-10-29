//! Persistence contract (SPEC.md section 6) and graceful shutdown (section 6, last bullets):
//! the real binary is started, written to, killed with SIGKILL (or signalled) and restarted.
//!
//! What a test *can* prove: process death. It cannot pull the power cord, so what
//! `always` buys over `everysec` on an OS crash is checked structurally instead, by
//! the fsync counters (`fsync_policy_is_observable_*`): under `always` the fsync has
//! happened before the barrier that releases the client; under `everysec` it has not.
#![cfg(unix)]

mod common;

use common::*;
use redis_rs::aof::{encode_record, Aof, FsyncPolicy};
use redis_rs::expiry::now_ms;
use redis_rs::resp::{encode_request, parse_request, Frame};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

fn aof_flags(dir: &Path, fsync: &str) -> Vec<String> {
    ["--appendonly", "yes", "--appendfsync", fsync, "--dir", dir.to_str().unwrap()]
        .iter()
        .map(ToString::to_string)
        .collect()
}

fn start(dir: &Path, fsync: &str) -> TestServer {
    start_with(dir, fsync, &[])
}

fn start_with(dir: &Path, fsync: &str, extra: &[&str]) -> TestServer {
    let mut flags = aof_flags(dir, fsync);
    flags.extend(extra.iter().map(ToString::to_string));
    let refs: Vec<&str> = flags.iter().map(String::as_str).collect();
    TestServer::start(&refs, dir)
}

fn aof_path(dir: &Path) -> PathBuf {
    dir.join("appendonly.aof")
}

/// SET key:{i} = value:{i} for i in range, pipelined in batches (every reply is read).
fn write_keys(c: &mut Client, range: std::ops::Range<usize>) {
    for chunk in range.collect::<Vec<_>>().chunks(50) {
        let mut wire = Vec::new();
        for i in chunk {
            encode_request(&[b"SET", format!("key:{i}").as_bytes(), format!("value:{i}").as_bytes()], &mut wire);
        }
        c.send_raw(&wire);
        for _ in chunk {
            assert_eq!(c.read_frame(), ok());
        }
    }
}

/// Which of `key:{i}` exist (pipelined GETs). Panics if a present key has the wrong value.
fn present_keys(c: &mut Client, range: std::ops::Range<usize>) -> Vec<usize> {
    let mut present = Vec::new();
    for chunk in range.collect::<Vec<_>>().chunks(500) {
        let mut wire = Vec::new();
        for i in chunk {
            encode_request(&[b"GET", format!("key:{i}").as_bytes()], &mut wire);
        }
        c.send_raw(&wire);
        for i in chunk {
            let got = c.read_frame();
            if got != Frame::Null {
                assert_eq!(got, bulk(&format!("value:{i}")), "key:{i} came back with the wrong value");
                present.push(*i);
            }
        }
    }
    present
}

fn read_aof(dir: &Path) -> Vec<Vec<String>> {
    let bytes = std::fs::read(aof_path(dir)).unwrap_or_default();
    let mut out = Vec::new();
    let mut pos = 0;
    while let Some((args, used)) = parse_request(&bytes[pos..]).expect("AOF must be valid RESP") {
        out.push(args.iter().map(|a| String::from_utf8_lossy(a).into_owned()).collect());
        pos += used;
    }
    assert_eq!(pos, bytes.len(), "trailing bytes in AOF");
    out
}

fn int_of(f: Frame) -> i64 {
    match f {
        Frame::Integer(n) => n,
        other => panic!("expected integer, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// kill -9 + restart, under each fsync policy
// ---------------------------------------------------------------------------

#[test]
fn kill9_under_always_keeps_all_1000_keys() {
    let dir = tempfile::tempdir().unwrap();
    let mut server = start(dir.path(), "always");
    write_keys(&mut server.client(), 0..1000);
    server.kill9();

    let server = start(dir.path(), "always");
    let mut c = server.client();
    assert_eq!(present_keys(&mut c, 0..1000).len(), 1000, "an acknowledged write was lost under appendfsync always");
    assert!(server.stderr().contains("replayed 1000 commands"), "{}", server.stderr());
}

#[test]
fn kill9_under_everysec_keeps_at_least_every_fsynced_key() {
    let dir = tempfile::tempdir().unwrap();
    let mut server = start(dir.path(), "everysec");
    let mut c = server.client();
    write_keys(&mut c, 0..1000);
    // > 1 s: the background fsync thread has certainly run since the first 1000 were written
    std::thread::sleep(Duration::from_millis(1300));
    write_keys(&mut c, 1000..2000);
    server.kill9(); // immediately: the second batch is NOT guaranteed fsynced

    let server = start(dir.path(), "everysec");
    let mut c = server.client();
    let first = present_keys(&mut c, 0..1000).len();
    let second = present_keys(&mut c, 1000..2000).len();
    assert_eq!(first, 1000, "fsynced keys must survive");
    // Documented window: on *process* death only records still queued in-process can be lost
    // (microseconds); on OS crash up to ~1 s. Here we can only assert the lower bound.
    println!("everysec after kill -9: {first} fsynced keys + {second}/1000 unfsynced keys survived");
    assert!(first + second >= 1000);
}

#[test]
fn kill9_under_no_keeps_every_key_that_reached_the_kernel() {
    let dir = tempfile::tempdir().unwrap();
    let mut server = start(dir.path(), "no");
    write_keys(&mut server.client(), 0..1000);
    // never fsynced, but write(2) completes within microseconds of the reply; give the writer thread a beat
    std::thread::sleep(Duration::from_millis(300));
    server.kill9();

    let server = start(dir.path(), "no");
    let mut c = server.client();
    assert_eq!(
        present_keys(&mut c, 0..1000).len(),
        1000,
        "process death must not lose data that was already write(2)n (page cache survives kill -9)"
    );
}

// ---------------------------------------------------------------------------
// the fsync policy, observed through the AOF counters
// ---------------------------------------------------------------------------

fn wait_barrier(sink: &redis_rs::aof::AofSink) {
    let rx = sink.barrier().expect("writer alive");
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        tokio::time::timeout(Duration::from_secs(10), rx).await.expect("barrier timed out").unwrap();
    });
}

fn record(sink: &redis_rs::aof::AofSink, n: usize) {
    for i in 0..n {
        sink.record(encode_record(&[b"SET", format!("k{i}").as_bytes(), b"v"]));
    }
}

#[test]
fn fsync_policy_is_observable_always_syncs_before_the_barrier_releases() {
    use std::sync::atomic::Ordering::Relaxed;
    let dir = tempfile::tempdir().unwrap();
    let mut aof = Aof::open(&aof_path(dir.path()), FsyncPolicy::Always).unwrap();
    let (sink, stats) = (aof.sink(), aof.stats());
    record(&sink, 5);
    wait_barrier(&sink);
    assert_eq!(stats.records.load(Relaxed), 5);
    assert!(stats.fsyncs.load(Relaxed) >= 1, "always: the client was released before any fsync");
    // group commit: 5 records must not cost 5 fsyncs
    assert!(stats.fsyncs.load(Relaxed) <= 5);
    aof.shutdown();
}

#[test]
fn fsync_policy_is_observable_everysec_syncs_in_the_background_not_inline() {
    use std::sync::atomic::Ordering::Relaxed;
    let dir = tempfile::tempdir().unwrap();
    let mut aof = Aof::open(&aof_path(dir.path()), FsyncPolicy::Everysec).unwrap();
    let (sink, stats) = (aof.sink(), aof.stats());
    record(&sink, 5);
    wait_barrier(&sink); // written, i.e. visible to the kernel...
    assert_eq!(stats.records.load(Relaxed), 5);
    assert_eq!(stats.fsyncs.load(Relaxed), 0, "everysec must not fsync inline");
    std::thread::sleep(Duration::from_millis(1400));
    assert!(stats.fsyncs.load(Relaxed) >= 1, "everysec: the background thread never synced");
    aof.shutdown();
}

#[test]
fn fsync_policy_is_observable_no_never_syncs_until_shutdown() {
    use std::sync::atomic::Ordering::Relaxed;
    let dir = tempfile::tempdir().unwrap();
    let mut aof = Aof::open(&aof_path(dir.path()), FsyncPolicy::No).unwrap();
    let (sink, stats) = (aof.sink(), aof.stats());
    record(&sink, 5);
    wait_barrier(&sink);
    std::thread::sleep(Duration::from_millis(1400));
    assert_eq!(stats.fsyncs.load(Relaxed), 0, "policy no must never fsync on its own");
    aof.shutdown();
    assert_eq!(stats.fsyncs.load(Relaxed), 1, "graceful shutdown does one final fsync");
    assert_eq!(std::fs::read(aof_path(dir.path())).unwrap().len() as u64, stats.bytes.load(Relaxed));
}

// ---------------------------------------------------------------------------
// AOF content
// ---------------------------------------------------------------------------

#[test]
fn aof_records_the_effect_of_each_write_in_redis_format() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_with(dir.path(), "always", &["--sweeper", "no"]);
    let mut c = server.client();
    let before = now_ms();
    c.cmd(&["SET", "k", "v"]);
    c.cmd(&["SET", "e", "v", "EX", "10"]);
    c.cmd(&["SET", "p", "v", "PX", "5000"]);
    c.cmd(&["EXPIRE", "k", "100"]);
    c.cmd(&["INCR", "n"]);
    c.cmd(&["HSET", "h", "f", "v", "g", "w"]);
    c.cmd(&["HDEL", "h", "f", "nope"]);
    c.cmd(&["DEL", "p"]);
    c.cmd(&["EXPIRE", "e", "0"]); // already due: journaled as a DEL, not a PEXPIREAT
                                  // none of these may be journaled:
    c.cmd(&["GET", "k"]);
    c.cmd(&["TTL", "k"]);
    c.cmd(&["EXISTS", "k"]);
    c.cmd(&["HGETALL", "h"]);
    c.cmd(&["DEL", "missing"]);
    c.cmd(&["EXPIRE", "missing", "5"]);
    c.cmd(&["HDEL", "h", "nope"]);
    c.cmd(&["INCR", "k"]); // k = "v": not an integer
    c.cmd(&["GET", "h"]); // wrong type
    c.cmd(&["HSET", "k", "f", "v"]); // wrong type
    c.cmd(&["FOO"]);
    let after = now_ms();

    let log = read_aof(dir.path());
    let to_ms = |s: &str| s.parse::<u64>().unwrap();
    assert_eq!(log.len(), 9, "unexpected records: {log:#?}");
    assert_eq!(log[0], ["SET", "k", "v"]);
    assert_eq!(&log[1][..4], ["SET", "e", "v", "PXAT"]);
    assert!((before + 10_000..=after + 10_000).contains(&to_ms(&log[1][4])), "absolute deadline: {log:?}");
    assert_eq!(&log[2][..4], ["SET", "p", "v", "PXAT"]);
    assert!((before + 5_000..=after + 5_000).contains(&to_ms(&log[2][4])));
    assert_eq!(&log[3][..2], ["PEXPIREAT", "k"]);
    assert!((before + 100_000..=after + 100_000).contains(&to_ms(&log[3][2])));
    assert_eq!(log[4], ["INCR", "n"]);
    assert_eq!(log[5], ["HSET", "h", "f", "v", "g", "w"]);
    assert_eq!(log[6], ["HDEL", "h", "f", "nope"]);
    assert_eq!(log[7], ["DEL", "p"]);
    assert_eq!(log[8], ["DEL", "e"]);
}

// ---------------------------------------------------------------------------
// expiry across restarts
// ---------------------------------------------------------------------------

#[test]
fn px_key_does_not_resurrect_after_downtime() {
    let dir = tempfile::tempdir().unwrap();
    let mut server = start(dir.path(), "always");
    let mut c = server.client();
    assert_eq!(c.cmd(&["SET", "px", "v", "PX", "200"]), ok());
    assert_eq!(c.cmd(&["SET", "viaexpire", "v"]), ok());
    assert_eq!(c.cmd(&["EXPIRE", "viaexpire", "1"]), int(1));
    assert_eq!(c.cmd(&["SET", "long", "v", "EX", "100"]), ok());
    assert_eq!(c.cmd(&["SET", "plain", "v"]), ok());
    assert_eq!(c.cmd(&["HSET", "hash", "f", "v"]), int(1));
    assert_eq!(c.cmd(&["EXPIRE", "hash", "1"]), int(1));
    server.kill9();

    std::thread::sleep(Duration::from_millis(1600)); // downtime: longer than every short TTL

    let server = start(dir.path(), "always");
    let mut c = server.client();
    assert_eq!(c.cmd(&["GET", "px"]), Frame::Null, "PX 200 key resurrected after 1.6 s of downtime");
    assert_eq!(c.cmd(&["GET", "viaexpire"]), Frame::Null, "EXPIRE 1 key resurrected");
    assert_eq!(c.cmd(&["HGET", "hash", "f"]), Frame::Null, "expiring hash resurrected");
    assert_eq!(c.cmd(&["EXISTS", "px"]), int(0));
    // deadlines are absolute: ~1.6 s of the 100 s are gone, not reset to a fresh 100
    let ttl = int_of(c.cmd(&["TTL", "long"]));
    assert!(
        (90..=99).contains(&ttl),
        "TTL after replay is {ttl}: relative TTL was replayed instead of the absolute deadline"
    );
    assert_eq!(c.cmd(&["TTL", "plain"]), int(-1));
    assert_eq!(c.cmd(&["GET", "plain"]), bulk("v"));
}

fn expired_key_then_incr_survives_restart(sweeper: &str) {
    // At runtime: k expires, then INCR starts a brand-new key "1" without TTL.
    // If expiry deletions were not journaled, replay (which never expires anything
    // while loading) would apply that INCR to the *old* "5" and then drop it as expired.
    let dir = tempfile::tempdir().unwrap();
    let mut server = start_with(dir.path(), "always", &["--sweeper", sweeper]);
    let mut c = server.client();
    assert_eq!(c.cmd(&["SET", "k", "5", "PX", "100"]), ok());
    assert_eq!(c.cmd(&["INCR", "k"]), int(6)); // still alive: TTL preserved
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(c.cmd(&["INCR", "k"]), int(1), "expired key must restart from scratch");
    assert_eq!(c.cmd(&["TTL", "k"]), int(-1));
    server.kill9();

    let server = start(dir.path(), "always");
    let mut c = server.client();
    assert_eq!(c.cmd(&["GET", "k"]), bulk("1"), "replay diverged from the pre-crash state (sweeper={sweeper})");
    assert_eq!(c.cmd(&["TTL", "k"]), int(-1));
}

#[test]
fn replay_matches_runtime_when_an_expired_key_is_reused_lazy_path() {
    expired_key_then_incr_survives_restart("no");
}

#[test]
fn replay_matches_runtime_when_an_expired_key_is_reused_sweeper_path() {
    expired_key_then_incr_survives_restart("yes");
}

// ---------------------------------------------------------------------------
// torn tail: policy (b) = truncate, log, start
// ---------------------------------------------------------------------------

fn base_log(n: usize) -> (Vec<u8>, Vec<u8>) {
    // n complete records, then the record that will be cut
    let mut base = Vec::new();
    for i in 0..n {
        encode_request(&[b"SET", format!("key:{i}").as_bytes(), format!("value:{i}").as_bytes()], &mut base);
    }
    let mut last = Vec::new();
    encode_request(&[b"SET", format!("key:{n}").as_bytes(), format!("value:{n}").as_bytes()], &mut last);
    (base, last)
}

fn report_of(aof: &Path) -> String {
    let name = format!("{}.corruption-report.txt", aof.file_name().unwrap().to_string_lossy());
    std::fs::read_to_string(aof.with_file_name(name)).unwrap_or_default()
}

fn json_u64(report: &str, key: &str) -> u64 {
    let needle = format!("\"{key}\": ");
    let rest =
        &report[report.find(&needle).unwrap_or_else(|| panic!("no {key} in report:\n{report}")) + needle.len()..];
    rest.split([',', '\n']).next().unwrap().trim().parse().unwrap()
}

#[test]
fn a_torn_final_command_is_truncated_logged_and_serving_continues() {
    let dir = tempfile::tempdir().unwrap();
    let mut server = start(dir.path(), "always");
    write_keys(&mut server.client(), 0..10);
    server.kill9();

    // deliberately cut one byte off the end of the file
    let path = aof_path(dir.path());
    let len_before = std::fs::metadata(&path).unwrap().len();
    let (_, last) = base_log(9);
    let rec_len = last.len() as u64;
    std::fs::OpenOptions::new().write(true).open(&path).unwrap().set_len(len_before - 1).unwrap();

    let server = start(dir.path(), "always");
    let mut c = server.client();
    assert_eq!(present_keys(&mut c, 0..9).len(), 9, "complete commands before the tear must survive");
    assert_eq!(c.cmd(&["GET", "key:9"]), Frame::Null, "the torn command must not be half-applied");

    // it was logged, loudly...
    let stderr = server.stderr();
    assert!(stderr.contains("dropped") && stderr.contains("incomplete final command"), "{stderr}");
    // ... with a machine-readable report
    let report = report_of(&path);
    assert!(report.contains("\"action\": \"truncated_tail\""), "{report}");
    assert_eq!(json_u64(&report, "bytes_dropped"), rec_len - 1);
    assert_eq!(json_u64(&report, "last_good_offset"), len_before - rec_len);
    assert!(report.contains("\"offending_hex\": \"2a 33 0d 0a 24 33 0d 0a 53 45 54"), "{report}"); // "*3\r\n$3\r\nSET"
    std::fs::create_dir_all(results_dir()).unwrap();
    std::fs::write(results_dir().join("aof_corruption_report.txt"), &report).unwrap();

    // the file really was truncated to the last good byte, so the next start is clean
    assert_eq!(std::fs::metadata(&path).unwrap().len(), len_before - rec_len);
    assert_eq!(c.cmd(&["SET", "after", "tear"]), ok());
    drop(server);
    let _ = std::fs::remove_file(path.with_file_name("appendonly.aof.corruption-report.txt"));
    let server = start(dir.path(), "always");
    let mut c = server.client();
    assert_eq!(c.cmd(&["GET", "after"]), bulk("tear"));
    assert_eq!(c.cmd(&["GET", "key:9"]), Frame::Null);
    assert!(!server.stderr().contains("dropped"), "second start should be clean: {}", server.stderr());
    assert!(report_of(&path).is_empty(), "no report expected on a clean start");
}

#[test]
fn a_tear_at_every_byte_offset_of_the_last_record_is_handled() {
    let (base, last) = base_log(9);
    for cut in 0..=last.len() {
        let dir = tempfile::tempdir().unwrap();
        let mut bytes = base.clone();
        bytes.extend_from_slice(&last[..cut]);
        std::fs::write(aof_path(dir.path()), &bytes).unwrap();

        let server = start(dir.path(), "no");
        let mut c = server.client();
        let present = present_keys(&mut c, 0..10);
        let complete = cut == last.len();
        assert_eq!(present.len(), if complete { 10 } else { 9 }, "cut {cut}/{}", last.len());
        let torn = cut != 0 && !complete;
        assert_eq!(server.stderr().contains("dropped"), torn, "cut {cut}: {}", server.stderr());
        if torn {
            assert_eq!(json_u64(&report_of(&aof_path(dir.path())), "bytes_dropped"), cut as u64, "cut {cut}");
            assert_eq!(std::fs::metadata(aof_path(dir.path())).unwrap().len(), base.len() as u64);
        }
    }
}

// ---------------------------------------------------------------------------
// everything else wrong with the file: refuse to start, touch nothing
// ---------------------------------------------------------------------------

fn assert_refuses(content: &[u8], needle: &str) -> String {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(aof_path(dir.path()), content).unwrap();
    let flags = aof_flags(dir.path(), "always");
    let refs: Vec<&str> = flags.iter().map(String::as_str).collect();
    let failure = match TestServer::try_start(&refs, dir.path()) {
        Ok(_) => panic!("server started on a corrupt AOF ({needle})"),
        Err(f) => f,
    };
    assert_eq!(failure.status.and_then(|s| s.code()), Some(1), "{}", failure.stderr);
    assert!(failure.stderr.contains("refusing to start") && failure.stderr.contains(needle), "{}", failure.stderr);
    assert_eq!(std::fs::read(aof_path(dir.path())).unwrap(), content, "a refused AOF must not be modified");
    let report = report_of(&aof_path(dir.path()));
    assert!(report.contains("\"action\": \"refused_to_start\""), "{report}");
    report
}

#[test]
fn garbage_in_the_middle_refuses_to_start_and_leaves_the_file_alone() {
    let (base, last) = base_log(3);
    let mut content = base.clone();
    content.extend_from_slice(b"XXXX garbage XXXX\r\n");
    content.extend_from_slice(&last);
    let report = assert_refuses(&content, "invalid RESP");
    assert_eq!(json_u64(&report, "last_good_offset"), base.len() as u64);
}

#[test]
fn a_record_that_cannot_be_re_executed_refuses_to_start() {
    let mut content = Vec::new();
    encode_request(&[b"SET", b"a", b"1"], &mut content);
    let good = content.len();
    encode_request(&[b"FOO", b"bar"], &mut content); // valid RESP, unknown command
    let report = assert_refuses(&content, "failed to re-execute");
    assert_eq!(json_u64(&report, "last_good_offset"), good as u64);

    let mut content = Vec::new();
    encode_request(&[b"SET", b"a", b"1", b"EX", b"10"], &mut content); // relative expiry has no place in an AOF
    assert_refuses(&content, "failed to re-execute");
}

// ---------------------------------------------------------------------------
// graceful shutdown
// ---------------------------------------------------------------------------

#[test]
fn sigint_exits_zero_and_flushes_every_acknowledged_write() {
    for policy in ["everysec", "no"] {
        let dir = tempfile::tempdir().unwrap();
        let mut server = start(dir.path(), policy);
        write_keys(&mut server.client(), 0..1000); // every one acknowledged...
        server.signal(libc::SIGINT); // ...but under everysec/no not necessarily fsynced yet
        let status = server.wait_exit(Duration::from_secs(10)).expect("server did not exit after SIGINT");
        assert_eq!(status.code(), Some(0), "policy {policy}: exit status {status:?}");
        assert!(server.stderr().contains("shutdown complete"), "{}", server.stderr());
        assert!(std::net::TcpStream::connect(server.addr).is_err(), "listener still open after shutdown");

        let server = start(dir.path(), policy);
        let mut c = server.client();
        assert_eq!(present_keys(&mut c, 0..1000).len(), 1000, "policy {policy}: data lost across graceful shutdown");
    }
}

#[test]
fn sigterm_behaves_like_sigint() {
    let dir = tempfile::tempdir().unwrap();
    let mut server = start(dir.path(), "everysec");
    write_keys(&mut server.client(), 0..200);
    server.signal(libc::SIGTERM);
    let status = server.wait_exit(Duration::from_secs(10)).expect("no exit after SIGTERM");
    assert_eq!(status.code(), Some(0));
    let server = start(dir.path(), "everysec");
    assert_eq!(present_keys(&mut server.client(), 0..200).len(), 200);
}

#[test]
fn sigint_with_commands_in_flight_loses_nothing_that_was_acknowledged() {
    let dir = tempfile::tempdir().unwrap();
    let mut server = start(dir.path(), "everysec");
    let mut c = server.client();
    let mut wire = Vec::new();
    for i in 0..50_000 {
        encode_request(&[b"SET", format!("key:{i}").as_bytes(), format!("value:{i}").as_bytes()], &mut wire);
    }
    let mut writer = c.try_clone_for_write();
    let sender = std::thread::spawn(move || {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| writer.send_raw(&wire)));
    });
    std::thread::sleep(Duration::from_millis(5));
    server.signal(libc::SIGINT);
    // count the replies we get before the server closes the connection
    let raw = c.read_to_close(Duration::from_secs(10));
    let acked = raw.windows(5).filter(|w| *w == b"+OK\r\n").count();
    let _ = sender.join();
    let status = server.wait_exit(Duration::from_secs(10)).expect("no exit");
    assert_eq!(status.code(), Some(0));

    let server = start(dir.path(), "everysec");
    let mut c = server.client();
    let present = present_keys(&mut c, 0..50_000);
    println!("in-flight shutdown: {acked} acknowledged, {} present after restart", present.len());
    // commands are applied in order, so exactly the first `acked` keys must be there
    assert!(present.len() >= acked, "{acked} acknowledged but only {} survived", present.len());
    assert!(present.iter().take(acked).copied().eq(0..acked));
}

#[test]
fn sigint_immediately_after_ready_still_exits_zero() {
    // regression guard for the handler-registration race: signals are installed before "Ready" is printed
    for round in 0..15 {
        let dir = tempfile::tempdir().unwrap();
        let mut server = TestServer::start(&[], dir.path());
        server.signal(libc::SIGINT);
        let status = server.wait_exit(Duration::from_secs(10)).expect("no exit");
        assert_eq!(status.code(), Some(0), "round {round}: {}", server.stderr());
    }
}

#[test]
fn shutdown_closes_idle_connections_promptly() {
    let dir = tempfile::tempdir().unwrap();
    let mut server = TestServer::start(&["--drain-timeout-ms", "20000"], dir.path());
    let mut idle: Vec<Client> = (0..20).map(|_| server.client()).collect();
    assert_eq!(idle[0].cmd(&["PING"]), Frame::simple("PONG"));
    let t = Instant::now();
    server.signal(libc::SIGINT);
    let status = server.wait_exit(Duration::from_secs(10)).expect("idle connections blocked shutdown");
    assert_eq!(status.code(), Some(0));
    assert!(t.elapsed() < Duration::from_secs(3), "took {:?}", t.elapsed());
}

#[test]
fn a_second_signal_forces_exit_when_the_drain_is_stuck() {
    let dir = tempfile::tempdir().unwrap();
    let mut server = TestServer::start(&["--drain-timeout-ms", "60000"], dir.path());
    let mut c = server.client();
    let big = vec![b'x'; 1024 * 1024];
    assert_eq!(c.cmd(&[&b"SET"[..], b"big", &big]), ok());
    // pipeline many GETs of a 1 MiB value and never read: the server blocks writing replies
    let mut wire = Vec::new();
    for _ in 0..300 {
        encode_request(&[b"GET", b"big"], &mut wire);
    }
    let mut writer = c.try_clone_for_write();
    std::thread::spawn(move || {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| writer.send_raw(&wire)));
    });
    std::thread::sleep(Duration::from_millis(500));

    server.signal(libc::SIGINT);
    assert!(server.wait_exit(Duration::from_millis(1500)).is_none(), "should still be draining the stuck connection");
    server.signal(libc::SIGINT);
    let status = server.wait_exit(Duration::from_secs(5)).expect("second SIGINT did not force an exit");
    assert_eq!(status.code(), Some(1));
    drop(c);
}
