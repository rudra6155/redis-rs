//! Error semantics (SPEC.md section 3): exact error prefix and message.
//!
//! * `error_strings_are_exact` needs no Docker: the expected strings are
//!   constants that `oracle_error_strings_match_redis7` re-verifies against a real
//!   `redis:7` on every run, so the constants cannot silently rot.
//! * `oracle_error_strings_match_redis7` (needs Docker, or ORACLE_ADDR) sends the
//!   same failing command to both servers and compares the raw reply bytes.

mod common;

use common::*;

struct Case {
    name: &'static str,
    setup: &'static [&'static [&'static str]],
    cmd: &'static [&'static str],
    /// Full error line without the leading `-` and trailing CRLF (as measured on redis 7.4.12).
    expected: &'static str,
}

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

const CASES: &[Case] = &[
    // --- the four required cases ---
    Case {
        name: "unknown command",
        setup: &[],
        cmd: &["FOO"],
        expected: "ERR unknown command 'FOO', with args beginning with: ",
    },
    Case {
        name: "unknown command with args",
        setup: &[],
        cmd: &["FOO", "a", "b"],
        expected: "ERR unknown command 'FOO', with args beginning with: 'a' 'b' ",
    },
    Case {
        name: "arity: GET with no args",
        setup: &[],
        cmd: &["GET"],
        expected: "ERR wrong number of arguments for 'get' command",
    },
    Case {
        name: "wrong type: GET on a hash",
        setup: &[&["HSET", "h", "f", "v"]],
        cmd: &["GET", "h"],
        expected: WRONGTYPE,
    },
    Case {
        name: "wrong type: HSET on a string",
        setup: &[&["SET", "s", "x"]],
        cmd: &["HSET", "s", "f", "v"],
        expected: WRONGTYPE,
    },
    Case {
        name: "INCR on a non-integer",
        setup: &[&["SET", "s", "abc"]],
        cmd: &["INCR", "s"],
        expected: "ERR value is not an integer or out of range",
    },
    // --- neighbours ---
    Case {
        name: "arity: SET with one arg",
        setup: &[],
        cmd: &["SET", "k"],
        expected: "ERR wrong number of arguments for 'set' command",
    },
    Case {
        name: "arity: HSET with an odd field/value tail",
        setup: &[],
        cmd: &["HSET", "h", "f", "v", "g"],
        expected: "ERR wrong number of arguments for 'hset' command",
    },
    Case {
        name: "INCR overflow",
        setup: &[&["SET", "big", "9223372036854775807"]],
        cmd: &["INCR", "big"],
        expected: "ERR increment or decrement would overflow",
    },
    Case { name: "INCR on a hash", setup: &[&["HSET", "h", "f", "v"]], cmd: &["INCR", "h"], expected: WRONGTYPE },
    Case {
        name: "SET EX 0",
        setup: &[],
        cmd: &["SET", "k", "v", "EX", "0"],
        expected: "ERR invalid expire time in 'set' command",
    },
    Case {
        name: "SET EX not a number",
        setup: &[],
        cmd: &["SET", "k", "v", "EX", "abc"],
        expected: "ERR value is not an integer or out of range",
    },
    Case {
        name: "SET with an unknown option",
        setup: &[],
        cmd: &["SET", "k", "v", "FOO"],
        expected: "ERR syntax error",
    },
    Case {
        name: "EXPIRE with a non-integer, key missing",
        setup: &[],
        cmd: &["EXPIRE", "missing", "abc"],
        expected: "ERR value is not an integer or out of range",
    },
    Case {
        name: "EXPIRE overflow",
        setup: &[&["SET", "k", "v"]],
        cmd: &["EXPIRE", "k", "9223372036854775807"],
        expected: "ERR invalid expire time in 'expire' command",
    },
];

fn run_case(c: &mut Client, case: &Case) -> Vec<u8> {
    for setup in case.setup {
        let _ = c.raw_cmd(setup);
    }
    c.raw_cmd(case.cmd)
}

#[test]
fn error_strings_are_exact() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start(&[], dir.path());
    for case in CASES {
        // fresh connection is not enough (keys persist), so use a unique keyspace per case via a fresh server
        let mut c = server.client();
        let reply = run_case(&mut c, case);
        assert_eq!(escape(&reply), format!("-{}\\r\\n", case.expected), "case '{}': {:?}", case.name, case.cmd);
        // clean up keys the setup created so cases stay independent
        for key in ["h", "s", "big", "k", "missing"] {
            let _ = c.raw_cmd(&["DEL", key]);
        }
    }
}

#[test]
fn oracle_error_strings_match_redis7() {
    let oracle = Oracle::start();
    println!("oracle: redis_version={} via {}", oracle.version, oracle.source);
    assert!(oracle.version.starts_with("7."), "expected a redis 7.x oracle, got {}", oracle.version);

    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start(&[], dir.path());
    let mut mine = server.client();
    let mut theirs = oracle.client();

    for case in CASES {
        let m = run_case(&mut mine, case);
        let t = run_case(&mut theirs, case);
        assert_eq!(
            escape(&m),
            escape(&t),
            "case '{}' {:?}: redis-rs and redis {} disagree",
            case.name,
            case.cmd,
            oracle.version
        );
        // exact prefix (the error type word) and message, matching Redis byte-for-byte
        assert!(m.starts_with(b"-"), "not an error reply: {:?}", escape(&m));
        let prefix = |r: &[u8]| String::from_utf8_lossy(&r[1..]).split(' ').next().unwrap().to_string();
        assert_eq!(prefix(&m), prefix(&t), "error prefix, case '{}'", case.name);
        // the constant in this file must still be what the real Redis says
        assert_eq!(
            escape(&t),
            format!("-{}\\r\\n", case.expected),
            "oracle no longer says what CASES expects (case '{}')",
            case.name
        );
        for key in ["h", "s", "big", "k", "missing"] {
            let _ = mine.raw_cmd(&["DEL", key]);
            let _ = theirs.raw_cmd(&["DEL", key]);
        }
    }
}

/// Every divergence from Redis listed in SPEC.md section 8 behaves exactly as documented
/// (so the list cannot silently go stale in either direction).
#[test]
fn documented_divergences_behave_as_documented() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start(&[], dir.path());
    let mut c = server.client();
    let text = |f: redis_rs::resp::Frame| match f {
        redis_rs::resp::Frame::Error(e) => String::from_utf8_lossy(&e).into_owned(),
        other => panic!("expected an error reply, got {other:?}"),
    };

    // D4: SET options other than EX/PX
    for opt in [&["NX"][..], &["XX"], &["GET"], &["KEEPTTL"], &["EXAT", "5"], &["PXAT", "5"]] {
        let mut args = vec!["SET", "k", "v"];
        args.extend_from_slice(opt);
        assert_eq!(text(c.cmd(&args)), "ERR syntax error", "SET ... {opt:?}");
    }
    // D5: single-key DEL / EXISTS
    assert_eq!(text(c.cmd(&["DEL", "a", "b"])), "ERR wrong number of arguments for 'del' command");
    assert_eq!(text(c.cmd(&["EXISTS", "a", "b"])), "ERR wrong number of arguments for 'exists' command");
    // D6: no EXPIRE options
    assert_eq!(text(c.cmd(&["EXPIRE", "k", "10", "NX"])), "ERR wrong number of arguments for 'expire' command");
    // D7 / out-of-scope commands answer like any unknown command
    for cmd in
        ["FLUSHALL", "CONFIG", "INFO", "SELECT", "QUIT", "CLIENT", "DBSIZE", "KEYS", "SCAN", "SUBSCRIBE", "MULTI"]
    {
        let reply = text(c.cmd(&[cmd]));
        assert!(
            reply.starts_with(&format!("ERR unknown command '{cmd}', with args beginning with:")),
            "{cmd}: {reply}"
        );
    }
    // the AOF-only forms are invisible to clients (they are not in scope either)
    assert!(text(c.cmd(&["PEXPIREAT", "k", "5"])).starts_with("ERR unknown command 'PEXPIREAT'"));
    // D1 and D3 are asserted in resp_unit.rs; D2:
    let mut over = server.client();
    over.send_raw(b"*1048577\r\n");
    assert_eq!(
        escape(&over.read_to_close(std::time::Duration::from_secs(5))),
        "-ERR Protocol error: invalid multibulk length\\r\\n"
    );
}
