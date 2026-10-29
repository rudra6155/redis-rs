//! The oracle test (highest-value test in the repo).
//!
//! A fixed script of commands is sent to **redis-rs** and to a real **redis:7**
//! (Docker, or ORACLE_ADDR); the raw reply bytes are compared. A second section
//! does the same at the wire level (pipelining, split segments, CRLF inside bulk
//! strings, malformed requests). Only genuinely non-deterministic values (the
//! remaining TTL in seconds) are normalised before diffing.
//!
//! The result is written to `results/oracle_report.txt` on every run (pass or
//! fail) before the assertion, so the committed report is always a real run.
//!
//! Deliberate divergences (SPEC.md section 8) are *not* in the script; they are
//! listed at the bottom of the report.

mod common;

use common::*;
use std::fmt::Write as _;
use std::time::Duration;

#[derive(Clone, Copy)]
enum Norm {
    /// compare byte for byte
    Exact,
    /// reply is `:<secs>`; accept any value within +-1 of `n` on both servers
    TtlNear(i64),
}

enum Step {
    Cmd(Vec<Vec<u8>>, Norm),
    Sleep(u64),
}

fn c(args: &[&str]) -> Step {
    Step::Cmd(args.iter().map(|a| a.as_bytes().to_vec()).collect(), Norm::Exact)
}

fn cb(args: &[&[u8]]) -> Step {
    Step::Cmd(args.iter().map(|a| a.to_vec()).collect(), Norm::Exact)
}

fn ttl(args: &[&str], near: i64) -> Step {
    Step::Cmd(args.iter().map(|a| a.as_bytes().to_vec()).collect(), Norm::TtlNear(near))
}

fn script() -> Vec<Step> {
    let long = "x".repeat(200);
    let hundred = "y".repeat(100);
    vec![
        // ---- PING / ECHO ----
        c(&["PING"]),
        c(&["PING", "hello"]),
        c(&["ping"]),
        c(&["ECHO", "world"]),
        cb(&[b"ECHO", b"a\r\nb\0c"]),
        c(&["ECHO", ""]),
        // ---- error case 1: unknown command ----
        c(&["FOO"]),
        c(&["FOO", "a", "b"]),
        c(&["foo", "1", "2", "3"]),
        c(&["FOO", &long]),
        c(&["FOO", &hundred, &hundred]),
        c(&[&long]),
        // ---- error case 2: arity ----
        c(&["GET"]),
        c(&["GET", "a", "b"]),
        c(&["SET"]),
        c(&["SET", "k"]),
        c(&["DEL"]),
        c(&["EXISTS"]),
        c(&["TTL"]),
        c(&["TTL", "a", "b"]),
        c(&["EXPIRE", "k"]),
        c(&["EXPIRE"]),
        c(&["INCR"]),
        c(&["INCR", "a", "b"]),
        c(&["HSET"]),
        c(&["HSET", "h", "f"]),
        c(&["HSET", "h", "f", "v", "g"]),
        c(&["HGET", "h"]),
        c(&["HDEL", "h"]),
        c(&["HLEN"]),
        c(&["HGETALL"]),
        c(&["PING", "a", "b"]),
        c(&["ECHO"]),
        c(&["ECHO", "a", "b"]),
        // ---- SET / GET ----
        c(&["SET", "k", "v"]),
        c(&["GET", "k"]),
        c(&["GET", "missing"]),
        c(&["SET", "k", "v2"]),
        c(&["GET", "k"]),
        c(&["set", "lower", "case"]),
        c(&["gEt", "lower"]),
        cb(&[b"SET", b"bin", b"\x00\x01\r\n\xff"]),
        c(&["GET", "bin"]),
        c(&["SET", "empty", ""]),
        c(&["GET", "empty"]),
        // ---- SET EX / PX ----
        c(&["SET", "e1", "v", "EX", "100"]),
        ttl(&["TTL", "e1"], 100),
        c(&["SET", "e2", "v", "PX", "100500"]),
        ttl(&["TTL", "e2"], 100),
        c(&["SET", "e3", "v", "ex", "100"]),
        ttl(&["TTL", "e3"], 100),
        c(&["SET", "e4", "v", "EX", "10", "EX", "20"]),
        ttl(&["TTL", "e4"], 20),
        // TTL rounds to the nearest second (SPEC 4.1): remaining is 1500..1900 ms here, so the
        // exact answer is 2 for any scheduling delay under 400 ms; truncation would answer 1.
        c(&["SET", "round", "v", "PX", "1900"]),
        c(&["TTL", "round"]),
        c(&["SET", "k", "v", "EX", "0"]),
        c(&["SET", "k", "v", "EX", "-1"]),
        c(&["SET", "k", "v", "PX", "0"]),
        c(&["SET", "k", "v", "EX", "abc"]),
        c(&["SET", "k", "v", "EX", "1.5"]),
        c(&["SET", "k", "v", "EX", "+5"]),
        c(&["SET", "k", "v", "EX", ""]),
        c(&["SET", "k", "v", "EX", "9223372036854775807"]),
        c(&["SET", "k", "v", "PX", "9223372036854775807"]),
        c(&["SET", "k", "v", "EX", "10", "PX", "100"]),
        c(&["SET", "k", "v", "PX", "100", "EX", "10"]),
        c(&["SET", "k", "v", "EX"]),
        c(&["SET", "k", "v", "PX"]),
        c(&["SET", "k", "v", "FOO"]),
        c(&["SET", "k", "v", "EX", "abc", "FOO"]),
        // SET without EX/PX clears an existing TTL
        c(&["SET", "t", "v", "EX", "100"]),
        c(&["SET", "t", "v2"]),
        c(&["TTL", "t"]),
        // ---- TTL: -2 / -1 / n ----
        c(&["TTL", "nosuchkey"]),
        c(&["SET", "nottl", "v"]),
        c(&["TTL", "nottl"]),
        c(&["EXPIRE", "nottl", "100"]),
        ttl(&["TTL", "nottl"], 100),
        // ---- EXPIRE ----
        c(&["EXPIRE", "nottl", "200"]),
        ttl(&["TTL", "nottl"], 200),
        c(&["EXPIRE", "missing", "10"]),
        c(&["EXPIRE", "nottl", "abc"]),
        c(&["EXPIRE", "missing", "abc"]),
        c(&["EXPIRE", "nottl", "9223372036854775807"]),
        c(&["EXPIRE", "nottl", "-9223372036854775808"]),
        c(&["EXPIRE", "nottl", "0"]),
        c(&["EXISTS", "nottl"]),
        c(&["SET", "neg", "v"]),
        c(&["EXPIRE", "neg", "-5"]),
        c(&["GET", "neg"]),
        c(&["SET", "p", "v", "PX", "5000"]),
        c(&["EXPIRE", "p", "100"]),
        ttl(&["TTL", "p"], 100),
        // ---- DEL / EXISTS ----
        c(&["SET", "d", "v"]),
        c(&["EXISTS", "d"]),
        c(&["DEL", "d"]),
        c(&["DEL", "d"]),
        c(&["EXISTS", "d"]),
        c(&["EXISTS", "never"]),
        // ---- INCR ----
        c(&["INCR", "c"]),
        c(&["INCR", "c"]),
        c(&["GET", "c"]),
        c(&["SET", "s", "abc"]),
        c(&["INCR", "s"]),
        c(&["SET", "big", "9223372036854775807"]),
        c(&["INCR", "big"]),
        c(&["SET", "b2", "9223372036854775806"]),
        c(&["INCR", "b2"]),
        c(&["INCR", "b2"]),
        c(&["SET", "neg5", "-5"]),
        c(&["INCR", "neg5"]),
        c(&["SET", "z", "007"]),
        c(&["INCR", "z"]),
        c(&["SET", "plus", "+5"]),
        c(&["INCR", "plus"]),
        c(&["SET", "sp", " 5"]),
        c(&["INCR", "sp"]),
        c(&["SET", "n0", "-0"]),
        c(&["INCR", "n0"]),
        c(&["SET", "fl", "1.5"]),
        c(&["INCR", "fl"]),
        c(&["SET", "emp", ""]),
        c(&["INCR", "emp"]),
        c(&["SET", "huge", "99999999999999999999"]),
        c(&["INCR", "huge"]),
        // INCR preserves the TTL
        c(&["SET", "ct", "5", "EX", "100"]),
        c(&["INCR", "ct"]),
        ttl(&["TTL", "ct"], 100),
        // ---- hashes ----
        c(&["HSET", "h", "f", "v"]),
        c(&["HSET", "h", "f", "v"]),
        c(&["HSET", "h", "f", "v2", "g", "w"]),
        c(&["HSET", "h", "a", "1", "a", "2"]),
        c(&["HGET", "h", "f"]),
        c(&["HGET", "h", "a"]),
        c(&["HGET", "h", "nope"]),
        c(&["HGET", "nokey", "f"]),
        c(&["HLEN", "h"]),
        c(&["HLEN", "nokey"]),
        c(&["HGETALL", "h"]),
        c(&["HGETALL", "nokey"]),
        c(&["HDEL", "h", "f"]),
        c(&["HDEL", "h", "nope"]),
        c(&["HGETALL", "h"]),
        c(&["HDEL", "h", "g", "a", "zzz"]),
        c(&["EXISTS", "h"]),
        c(&["HLEN", "h"]),
        c(&["HDEL", "h", "f"]),
        c(&["HDEL", "nokey", "f"]),
        // ---- hashes vs generic commands ----
        c(&["HSET", "ht", "a", "1"]),
        c(&["EXPIRE", "ht", "100"]),
        c(&["HSET", "ht", "b", "2"]),
        ttl(&["TTL", "ht"], 100),
        c(&["HDEL", "ht", "a", "b"]),
        c(&["TTL", "ht"]),
        c(&["HSET", "ht2", "a", "1"]),
        c(&["EXPIRE", "ht2", "100"]),
        c(&["SET", "ht2", "x"]),
        c(&["TTL", "ht2"]),
        c(&["HSET", "hd", "a", "1"]),
        c(&["DEL", "hd"]),
        c(&["DEL", "hd"]),
        // ---- error case 3: WRONGTYPE, both directions ----
        c(&["SET", "str", "x"]),
        c(&["HSET", "str", "f", "v"]),
        c(&["HGET", "str", "f"]),
        c(&["HLEN", "str"]),
        c(&["HGETALL", "str"]),
        c(&["HDEL", "str", "f"]),
        c(&["HSET", "hh", "f", "v"]),
        c(&["GET", "hh"]),
        c(&["INCR", "hh"]),
        c(&["SET", "hh", "now a string"]),
        c(&["HGET", "hh", "f"]),
        c(&["GET", "hh"]),
        // ---- expiry: lazy path ----
        // px is checked while still alive (generous 300 ms), the others only after the sleep
        c(&["SET", "px", "v", "PX", "300"]),
        c(&["EXISTS", "px"]),
        c(&["SET", "px2", "5", "PX", "30"]),
        c(&["SET", "px3", "v", "PX", "30"]),
        Step::Sleep(450),
        c(&["GET", "px"]),
        c(&["EXISTS", "px"]),
        c(&["TTL", "px"]),
        c(&["INCR", "px2"]),
        c(&["TTL", "px2"]),
        c(&["DEL", "px3"]),
        c(&["HSET", "px4", "f", "v"]),
        c(&["EXPIRE", "px4", "0"]),
        c(&["HGET", "px4", "f"]),
    ]
}

// ---- wire-level cases: fresh connection per case ----

struct RawCase {
    name: &'static str,
    /// segments sent with a pause between them (separate TCP segments)
    segments: Vec<Vec<u8>>,
    /// number of reply frames to read; ignored when `closes`
    replies: usize,
    /// server must reply with an error and close the connection
    closes: bool,
}

fn req(args: &[&[u8]]) -> Vec<u8> {
    let mut v = Vec::new();
    redis_rs::resp::encode_request(args, &mut v);
    v
}

fn raw_cases() -> Vec<RawCase> {
    let mut pipelined = req(&[b"PING"]);
    pipelined.extend(req(&[b"ECHO", b"hi"]));
    pipelined.extend(req(&[b"GET", b"raw:nokey"]));

    let mut crlf = req(&[b"SET", b"raw:crlf", b"a\r\nb\r\n"]);
    crlf.extend(req(&[b"GET", b"raw:crlf"]));

    let mut empty_then_ping = b"*0\r\n*-1\r\n".to_vec();
    empty_then_ping.extend(req(&[b"PING"]));

    let mut unknown_then_ping = req(&[b"FOO", b"x"]);
    unknown_then_ping.extend(req(&[b"PING"]));

    let set = req(&[b"SET", b"raw:split", b"value"]);

    vec![
        RawCase {
            name: "pipelined PING/ECHO/GET in one segment",
            segments: vec![pipelined],
            replies: 3,
            closes: false,
        },
        RawCase { name: "bulk string containing CRLF", segments: vec![crlf], replies: 2, closes: false },
        RawCase { name: "*0 and *-1 are skipped", segments: vec![empty_then_ping], replies: 1, closes: false },
        RawCase {
            name: "unknown command inside a pipeline",
            segments: vec![unknown_then_ping],
            replies: 2,
            closes: false,
        },
        RawCase {
            name: "command split mid-header",
            segments: vec![b"*1\r\n$4\r\nPI".to_vec(), b"NG\r\n".to_vec()],
            replies: 1,
            closes: false,
        },
        RawCase {
            name: "command split between CR and LF",
            segments: vec![b"*1\r\n$4\r\nPING\r".to_vec(), b"\n".to_vec()],
            replies: 1,
            closes: false,
        },
        RawCase {
            name: "SET split into three segments",
            segments: vec![set[..5].to_vec(), set[5..20].to_vec(), set[20..].to_vec()],
            replies: 1,
            closes: false,
        },
        RawCase { name: "protocol error: *abc", segments: vec![b"*abc\r\n".to_vec()], replies: 0, closes: true },
        RawCase {
            name: "protocol error: negative bulk length",
            segments: vec![b"*1\r\n$-5\r\n".to_vec()],
            replies: 0,
            closes: true,
        },
        RawCase {
            name: "protocol error: non-numeric bulk length",
            segments: vec![b"*1\r\n$abc\r\n".to_vec()],
            replies: 0,
            closes: true,
        },
        RawCase {
            name: "protocol error: bulk length over 512MB",
            segments: vec![b"*1\r\n$999999999999\r\n".to_vec()],
            replies: 0,
            closes: true,
        },
        RawCase {
            name: "protocol error: element is not a bulk string",
            segments: vec![b"*1\r\n+PING\r\n".to_vec()],
            replies: 0,
            closes: true,
        },
        RawCase {
            name: "protocol error: nested array in a request",
            segments: vec![b"*1\r\n*1\r\n$4\r\nPING\r\n".to_vec()],
            replies: 0,
            closes: true,
        },
    ]
}

fn run_raw(mut c: Client, case: &RawCase) -> Vec<u8> {
    for (i, seg) in case.segments.iter().enumerate() {
        if i > 0 {
            std::thread::sleep(Duration::from_millis(60));
        }
        c.send_raw(seg);
    }
    if case.closes {
        c.read_to_close(Duration::from_secs(2))
    } else {
        let mut out = Vec::new();
        for _ in 0..case.replies {
            out.extend(c.read_raw_frame());
        }
        out
    }
}

// ---- reporting ----

fn describe(args: &[Vec<u8>]) -> String {
    let mut s = args
        .iter()
        .map(|a| {
            let e = escape(a);
            if a.is_empty() {
                "\"\"".to_string()
            } else if e.len() > 24 {
                format!("{}...({}B)", &e[..16], a.len())
            } else {
                e
            }
        })
        .collect::<Vec<_>>()
        .join(" ");
    if s.len() > 70 {
        s.truncate(70);
        s.push_str("...");
    }
    s
}

fn parse_int_reply(r: &[u8]) -> Option<i64> {
    std::str::from_utf8(r).ok()?.strip_prefix(':')?.strip_suffix("\r\n")?.parse().ok()
}

/// Returns the strings that are compared (after normalisation).
fn normalise(norm: Norm, oracle: &[u8], mine: &[u8]) -> (String, String) {
    if let Norm::TtlNear(n) = norm {
        if let (Some(a), Some(b)) = (parse_int_reply(oracle), parse_int_reply(mine)) {
            if (a - n).abs() <= 1 && (b - n).abs() <= 1 {
                let token = format!(":~{n}");
                return (token.clone(), token);
            }
        }
    }
    (escape(oracle), escape(mine))
}

struct Row {
    n: usize,
    what: String,
    oracle: String,
    mine: String,
    pass: bool,
}

#[test]
fn oracle_script_and_wire_cases_match_redis7() {
    let oracle = Oracle::start();
    assert!(oracle.version.starts_with("7."), "expected redis 7.x, got {}", oracle.version);
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start(&[], dir.path());

    let mut rows: Vec<Row> = Vec::new();
    let mut mine = server.client();
    let mut theirs = oracle.client();

    for step in script() {
        match step {
            Step::Sleep(ms) => std::thread::sleep(Duration::from_millis(ms)),
            Step::Cmd(args, norm) => {
                let t = theirs.raw_cmd(&args);
                let m = mine.raw_cmd(&args);
                let (o_cmp, m_cmp) = normalise(norm, &t, &m);
                rows.push(Row {
                    n: rows.len() + 1,
                    what: describe(&args),
                    pass: o_cmp == m_cmp,
                    oracle: o_cmp,
                    mine: m_cmp,
                });
            }
        }
    }
    let script_rows = rows.len();

    for case in raw_cases() {
        let t = run_raw(oracle.client(), &case);
        let m = run_raw(server.client(), &case);
        rows.push(Row {
            n: rows.len() + 1,
            what: format!("[wire] {}", case.name),
            pass: t == m,
            oracle: escape(&t),
            mine: escape(&m),
        });
    }

    // ---- report ----
    let mut report = String::new();
    writeln!(report, "redis-rs protocol oracle report").unwrap();
    writeln!(report, "oracle : redis_version={} ({})", oracle.version, oracle.source).unwrap();
    writeln!(report, "subject: redis-rs {} (RESP2, no AOF)", env!("CARGO_PKG_VERSION")).unwrap();
    writeln!(report, "run at : {}", utc_iso(unix_now())).unwrap();
    writeln!(report, "method : same bytes sent to both servers; raw replies compared byte for byte.").unwrap();
    writeln!(report, "         Only `TTL` after EXPIRE/EX/PX is normalised (ttl within +-1 s of the expected value).")
        .unwrap();
    writeln!(report, "         Replies shown with \\r\\n visible.").unwrap();
    writeln!(report).unwrap();
    writeln!(report, "{:>4}  {:<5} {:<72} | oracle reply || redis-rs reply (shown only on FAIL)", "#", "", "request")
        .unwrap();
    for r in &rows {
        let verdict = if r.pass { "PASS" } else { "FAIL" };
        if r.pass {
            writeln!(report, "{:>4}  {verdict}  {:<72} | {}", r.n, r.what, r.oracle).unwrap();
        } else {
            writeln!(report, "{:>4}  {verdict}  {:<72} | oracle: {} || redis-rs: {}", r.n, r.what, r.oracle, r.mine)
                .unwrap();
        }
    }
    let failed = rows.iter().filter(|r| !r.pass).count();
    writeln!(report).unwrap();
    writeln!(
        report,
        "SUMMARY: {} rows ({} command rows + {} wire rows): {} PASS, {} FAIL",
        rows.len(),
        script_rows,
        rows.len() - script_rows,
        rows.len() - failed,
        failed
    )
    .unwrap();
    writeln!(report).unwrap();
    writeln!(report, "Deliberately NOT diffed (documented divergences, SPEC.md section 8):").unwrap();
    writeln!(report, "  D1 inline commands (`PING\\r\\n`) are rejected; Redis executes them").unwrap();
    writeln!(report, "  D2 requests with more than 1048576 arguments are rejected; Redis 7.4 accepts up to INT_MAX")
        .unwrap();
    writeln!(report, "  D3 a bulk payload not followed by CRLF is a protocol error; Redis skips 2 bytes blindly")
        .unwrap();
    writeln!(report, "  D4 SET options NX XX GET KEEPTTL EXAT PXAT answer `ERR syntax error`").unwrap();
    writeln!(report, "  D5 DEL/EXISTS accept exactly one key").unwrap();
    writeln!(report, "  D6 EXPIRE has no NX/XX/GT/LT options").unwrap();

    std::fs::create_dir_all(results_dir()).unwrap();
    std::fs::write(results_dir().join("oracle_report.txt"), &report).unwrap();
    println!("{report}");

    assert_eq!(failed, 0, "{failed} row(s) differ from redis {}; see results/oracle_report.txt", oracle.version);
}
