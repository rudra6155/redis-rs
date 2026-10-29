//! RESP2 contract (SPEC.md section 2):
//! every type round-trips; partial reads, pipelining and CRLF-in-bulk are handled,
//! both by the parsers directly and by the real server over TCP.

mod common;

use bytes::Bytes;
use common::*;
use redis_rs::resp::{encode_request, parse_request, Frame, ProtocolError, MAX_NESTING};
use std::time::Duration;

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

/// Tiny deterministic PRNG so failures are reproducible.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn sample_frames() -> Vec<(Frame, &'static [u8])> {
    vec![
        (Frame::simple("OK"), b"+OK\r\n"),
        (Frame::simple(""), b"+\r\n"),
        (Frame::error("ERR boom"), b"-ERR boom\r\n"),
        (Frame::Integer(0), b":0\r\n"),
        (Frame::Integer(-42), b":-42\r\n"),
        (Frame::Integer(i64::MAX), b":9223372036854775807\r\n"),
        (Frame::Integer(i64::MIN), b":-9223372036854775808\r\n"),
        (Frame::bulk(b("foo")), b"$3\r\nfoo\r\n"),
        (Frame::bulk(b("")), b"$0\r\n\r\n"),
        (Frame::bulk(b("a\r\nb")), b"$4\r\na\r\nb\r\n"),
        (Frame::Null, b"$-1\r\n"),
        (Frame::NullArray, b"*-1\r\n"),
        (Frame::Array(vec![]), b"*0\r\n"),
        (
            Frame::Array(vec![Frame::Integer(1), Frame::bulk(b("x")), Frame::Null, Frame::Array(vec![Frame::ok()])]),
            b"*4\r\n:1\r\n$1\r\nx\r\n$-1\r\n*1\r\n+OK\r\n",
        ),
    ]
}

// ---- 1. every type round-trips ----

#[test]
fn every_type_encodes_to_the_exact_wire_bytes_and_parses_back() {
    for (frame, wire) in sample_frames() {
        assert_eq!(frame.to_vec(), wire, "encode {frame:?}");
        let (parsed, used) = Frame::parse(wire).unwrap().unwrap();
        assert_eq!(parsed, frame, "parse {:?}", escape(wire));
        assert_eq!(used, wire.len());
    }
}

#[test]
fn null_bulk_and_null_array_are_distinct() {
    assert_eq!(Frame::parse(b"$-1\r\n").unwrap().unwrap().0, Frame::Null);
    assert_eq!(Frame::parse(b"*-1\r\n").unwrap().unwrap().0, Frame::NullArray);
    assert_ne!(Frame::Null, Frame::NullArray);
}

#[test]
fn random_frames_roundtrip_and_every_prefix_is_incomplete() {
    fn gen(rng: &mut Rng, depth: u32) -> Frame {
        let kinds = if depth == 0 { 6 } else { 7 };
        match rng.below(kinds) {
            0 => Frame::Simple(text(rng)),
            1 => Frame::Error(text(rng)),
            2 => Frame::Integer(rng.next() as i64),
            3 => Frame::Bulk(blob(rng)),
            4 => Frame::Null,
            5 => Frame::NullArray,
            _ => Frame::Array((0..rng.below(4)).map(|_| gen(rng, depth - 1)).collect()),
        }
    }
    // no CR/LF inside simple strings / errors (the encoder would sanitise them)
    fn text(rng: &mut Rng) -> Bytes {
        (0..rng.below(12)).map(|_| b'a' + rng.below(26) as u8).collect::<Vec<u8>>().into()
    }
    // binary-safe: any byte, including CR, LF and NUL
    fn blob(rng: &mut Rng) -> Bytes {
        (0..rng.below(24)).map(|_| rng.below(256) as u8).collect::<Vec<u8>>().into()
    }

    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    for _ in 0..2000 {
        let frame = gen(&mut rng, 3);
        let wire = frame.to_vec();
        let (parsed, used) = Frame::parse(&wire).unwrap().unwrap();
        assert_eq!(parsed, frame);
        assert_eq!(used, wire.len());
        for cut in 0..wire.len() {
            assert_eq!(
                Frame::parse(&wire[..cut]),
                Ok(None),
                "prefix {cut}/{} of {:?} must be 'incomplete'",
                wire.len(),
                escape(&wire)
            );
        }
    }
}

// ---- 2. partial reads ----

#[test]
fn request_parser_waits_for_more_bytes_at_every_split_point() {
    let mut wire = Vec::new();
    encode_request(&[b"SET", b"key", b"va\r\nlue"], &mut wire);
    for cut in 0..wire.len() {
        assert_eq!(parse_request(&wire[..cut]), Ok(None), "cut at {cut}");
    }
    let (args, used) = parse_request(&wire).unwrap().unwrap();
    assert_eq!(args, vec![&b"SET"[..], b"key", b"va\r\nlue"]);
    assert_eq!(used, wire.len());
}

// ---- 3. pipelining ----

#[test]
fn request_parser_splits_a_pipeline_into_commands() {
    let mut wire = Vec::new();
    encode_request(&[b"PING"], &mut wire);
    encode_request(&[b"ECHO", b"hi"], &mut wire);
    encode_request(&[b"GET", b"k"], &mut wire);
    let mut pos = 0;
    let mut seen = Vec::new();
    while let Some((args, used)) = parse_request(&wire[pos..]).unwrap() {
        seen.push(args.iter().map(|a| String::from_utf8_lossy(a).into_owned()).collect::<Vec<_>>());
        pos += used;
    }
    assert_eq!(pos, wire.len());
    assert_eq!(seen, vec![vec!["PING"], vec!["ECHO", "hi"], vec!["GET", "k"]]);
}

// ---- 4. bulk strings are length-prefixed, not delimiter-scanned ----

#[test]
fn bulk_payload_may_contain_crlf_and_arbitrary_bytes() {
    let payload: Vec<u8> = vec![b'a', b'\r', b'\n', b'*', b'1', b'\r', b'\n', 0, 255, b'$', b'-', b'1', b'\r', b'\n'];
    let mut wire = Vec::new();
    encode_request(&[b"SET", b"k", &payload], &mut wire);
    let (args, used) = parse_request(&wire).unwrap().unwrap();
    assert_eq!(args[2], &payload[..]);
    assert_eq!(used, wire.len());

    let frame = Frame::bulk(Bytes::from(payload.clone()));
    assert_eq!(Frame::parse(&frame.to_vec()).unwrap().unwrap().0, frame);
}

// ---- 5. protocol errors: wording copied from Redis, verified by protocol_oracle ----

#[test]
fn protocol_errors_have_redis_wording() {
    let cases: &[(&[u8], ProtocolError, &str)] = &[
        (b"*abc\r\n", ProtocolError::InvalidMultibulkLen, "Protocol error: invalid multibulk length"),
        (b"*1\r\n$-5\r\n", ProtocolError::InvalidBulkLen, "Protocol error: invalid bulk length"),
        (b"*1\r\n$abc\r\n", ProtocolError::InvalidBulkLen, "Protocol error: invalid bulk length"),
        (b"*1\r\n$999999999999\r\n", ProtocolError::InvalidBulkLen, "Protocol error: invalid bulk length"),
        (b"*1\r\n+PING\r\n", ProtocolError::ExpectedDollar(b'+'), "Protocol error: expected '$', got '+'"),
        (b"*1\r\n*1\r\n$4\r\nPING\r\n", ProtocolError::ExpectedDollar(b'*'), "Protocol error: expected '$', got '*'"),
    ];
    for (wire, expected, text) in cases {
        let err = parse_request(wire).unwrap_err();
        assert_eq!(&err, expected, "{:?}", escape(wire));
        assert_eq!(err.message(), *text);
    }
}

#[test]
fn inline_commands_are_rejected_not_executed() {
    assert_eq!(parse_request(b"PING\r\n"), Err(ProtocolError::InlineNotSupported));
    assert_eq!(
        ProtocolError::InlineNotSupported.message(),
        "Protocol error: inline commands are not supported (send RESP arrays)"
    );
}

#[test]
fn bulk_terminator_must_be_crlf() {
    assert_eq!(parse_request(b"*1\r\n$4\r\nPINGxx"), Err(ProtocolError::BadBulkTerminator));
    assert_eq!(Frame::parse(b"$3\r\nfooXY"), Err(ProtocolError::BadBulkTerminator));
}

#[test]
fn empty_requests_are_skipped_without_error() {
    assert_eq!(parse_request(b"*0\r\n"), Ok(Some((vec![], 4))));
    assert_eq!(parse_request(b"*-1\r\n"), Ok(Some((vec![], 5))));
}

// ---- 6. limits and hostile input ----

#[test]
fn oversized_declarations_fail_fast_without_buffering_the_body() {
    // header alone is enough: we must not wait for 600 MB of payload
    assert_eq!(parse_request(b"*1\r\n$600000000\r\n"), Err(ProtocolError::InvalidBulkLen));
    assert_eq!(parse_request(b"*1048577\r\n"), Err(ProtocolError::InvalidMultibulkLen));
    // a header line that never ends is cut off at 64 KiB
    let mut long = vec![b'*'];
    long.extend(std::iter::repeat_n(b'1', 70_000));
    assert_eq!(parse_request(&long), Err(ProtocolError::MbulkCountTooBig));
}

#[test]
fn deeply_nested_arrays_cannot_overflow_the_stack() {
    let mut wire = Vec::new();
    for _ in 0..100_000 {
        wire.extend_from_slice(b"*1\r\n");
    }
    assert_eq!(Frame::parse(&wire), Err(ProtocolError::NestingTooDeep));
    // the server-side parser never recurses: nesting is just "expected '$'"
    assert_eq!(parse_request(&wire), Err(ProtocolError::ExpectedDollar(b'*')));
    // ... and a legal depth still works
    let mut ok = Vec::new();
    for _ in 0..MAX_NESTING {
        ok.extend_from_slice(b"*1\r\n");
    }
    ok.extend_from_slice(b":1\r\n");
    assert!(Frame::parse(&ok).unwrap().is_some());
}

#[test]
fn array_header_does_not_preallocate_from_untrusted_length() {
    // 2 billion declared elements, none present: must be "incomplete", not an allocation failure
    assert_eq!(Frame::parse(b"*2000000000\r\n"), Ok(None));
}

#[test]
fn random_garbage_never_panics_and_never_over_consumes() {
    let alphabet: &[u8] = b"*$+-:\r\n0123456789abcPING";
    let mut rng = Rng(0x1234_5678_9ABC_DEF1);
    for _ in 0..50_000 {
        let len = rng.below(48) as usize;
        let buf: Vec<u8> = (0..len)
            .map(|_| {
                if rng.below(8) == 0 {
                    rng.below(256) as u8
                } else {
                    alphabet[rng.below(alphabet.len() as u64) as usize]
                }
            })
            .collect();
        if let Ok(Some((_, used))) = Frame::parse(&buf) {
            assert!(used <= buf.len());
        }
        if let Ok(Some((_, used))) = parse_request(&buf) {
            assert!(used <= buf.len());
        }
    }
}

// ---- 7. the same properties through the real server over TCP ----

fn server() -> (tempfile::TempDir, TestServer) {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start(&[], dir.path());
    (dir, server)
}

#[test]
fn server_handles_a_command_split_across_tcp_segments() {
    let (_dir, server) = server();
    let mut wire = Vec::new();
    encode_request(&[b"SET", b"split", b"va\r\nlue"], &mut wire);
    // Cut inside the header, inside a bulk header, inside a payload, and between CR and LF.
    let cuts = [2, 6, 13, 22, wire.len() - 1];
    for cut in cuts {
        let mut c = server.client();
        c.send_raw(&wire[..cut]);
        std::thread::sleep(Duration::from_millis(60)); // force a separate segment
        c.send_raw(&wire[cut..]);
        assert_eq!(c.read_frame(), ok(), "cut at {cut}");
        assert_eq!(c.cmd(&["GET", "split"]), bulk("va\r\nlue"));
    }
}

#[test]
fn server_handles_one_byte_at_a_time() {
    let (_dir, server) = server();
    let mut c = server.client();
    let mut wire = Vec::new();
    encode_request(&[b"ECHO", b"byte by byte"], &mut wire);
    for byte in &wire {
        c.send_raw(&[*byte]);
    }
    assert_eq!(c.read_frame(), bulk("byte by byte"));
}

#[test]
fn server_handles_pipelined_commands_in_one_segment() {
    let (_dir, server) = server();
    let mut c = server.client();
    let mut wire = Vec::new();
    encode_request(&[b"SET", b"p", b"1"], &mut wire);
    encode_request(&[b"INCR", b"p"], &mut wire);
    encode_request(&[b"GET", b"p"], &mut wire);
    encode_request(&[b"GET", b"missing"], &mut wire);
    encode_request(&[b"PING"], &mut wire);
    c.send_raw(&wire);
    assert_eq!(c.read_frame(), ok());
    assert_eq!(c.read_frame(), int(2));
    assert_eq!(c.read_frame(), bulk("2"));
    assert_eq!(c.read_frame(), Frame::Null);
    assert_eq!(c.read_frame(), Frame::simple("PONG"));
}

#[test]
fn server_handles_a_large_pipeline_in_order() {
    let (_dir, server) = server();
    let mut c = server.client();
    let mut wire = Vec::new();
    for i in 0..5000 {
        encode_request(&[b"SET", format!("k{i}").as_bytes(), format!("v{i}").as_bytes()], &mut wire);
        encode_request(&[b"GET", format!("k{i}").as_bytes()], &mut wire);
    }
    // writer thread so we never deadlock against a full socket buffer while reading replies
    let mut writer = c.try_clone_for_write();
    let handle = std::thread::spawn(move || writer.send_raw(&wire));
    for i in 0..5000 {
        assert_eq!(c.read_frame(), ok(), "reply to SET {i}");
        assert_eq!(c.read_frame(), bulk(&format!("v{i}")), "reply to GET {i}");
    }
    handle.join().unwrap();
}

#[test]
fn server_stores_a_value_containing_crlf_and_binary_bytes() {
    let (_dir, server) = server();
    let mut c = server.client();
    let payload: Vec<u8> = (0..=255u8).chain(b"\r\n\r\n".iter().copied()).collect();
    assert_eq!(c.cmd(&[&b"SET"[..], b"bin", &payload]), ok());
    assert_eq!(c.cmd(&["GET", "bin"]), Frame::bulk(Bytes::from(payload)));
}

#[test]
fn server_handles_a_value_larger_than_one_read() {
    let (_dir, server) = server();
    let mut c = server.client();
    let big = vec![b'z'; 5 * 1024 * 1024];
    assert_eq!(c.cmd(&[&b"SET"[..], b"big", &big]), ok());
    match c.cmd(&["GET", "big"]) {
        Frame::Bulk(v) => assert!(v.len() == big.len() && v.iter().all(|&x| x == b'z')),
        other => panic!("{other:?}"),
    }
}

#[test]
fn server_rejects_inline_commands_and_closes() {
    let (_dir, server) = server();
    let mut c = server.client();
    c.send_raw(b"PING\r\n");
    let reply = c.read_to_close(Duration::from_secs(5));
    assert_eq!(escape(&reply), "-ERR Protocol error: inline commands are not supported (send RESP arrays)\\r\\n");
}

#[test]
fn server_closes_the_connection_after_a_protocol_error_but_keeps_serving_others() {
    let (_dir, server) = server();
    let mut bad = server.client();
    bad.send_raw(b"*abc\r\n");
    assert_eq!(
        escape(&bad.read_to_close(Duration::from_secs(5))),
        "-ERR Protocol error: invalid multibulk length\\r\\n"
    );
    let mut good = server.client();
    assert_eq!(good.cmd(&["PING"]), Frame::simple("PONG"));
}

#[test]
fn server_skips_empty_requests_without_replying() {
    let (_dir, server) = server();
    let mut c = server.client();
    c.send_raw(b"*0\r\n*-1\r\n");
    assert_eq!(c.cmd(&["PING"]), Frame::simple("PONG")); // the very next reply is PONG
}
