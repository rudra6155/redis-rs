//! RESP2 wire format.
//!
//! Two decoders share the low-level helpers:
//!
//! * [`Frame::parse`] - general decoder for every RESP2 type (used by clients,
//!   tests and tools; recursion is bounded by [`MAX_NESTING`]).
//! * [`parse_request`] - the server's hot path. A request must be a flat array
//!   of bulk strings; it borrows the arguments straight out of the connection
//!   buffer (zero copy, no recursion, so hostile nesting cannot blow the stack).
//!
//! Both are *incremental*: `Ok(None)` means "valid so far, send me more bytes"
//! and nothing is consumed. Bulk strings are length-prefixed and never scanned
//! for delimiters, so payloads may contain `\r\n` and any other byte.

use bytes::Bytes;
use std::fmt;

/// Redis `proto-max-bulk-len` default.
pub const MAX_BULK_LEN: usize = 512 * 1024 * 1024;
/// Largest number of arguments accepted in one request (Redis 7.4 allows INT_MAX; see SPEC D2).
pub const MAX_REQUEST_ARGS: usize = 1024 * 1024;
/// Longest `*N` / `$N` header line we are willing to buffer while waiting for CRLF (Redis: 64 KiB).
pub const MAX_LINE_LEN: usize = 64 * 1024;
/// Deepest array nesting accepted by [`Frame::parse`].
pub const MAX_NESTING: usize = 32;

/// One RESP2 value.
///
/// String-like variants hold raw bytes: Redis strings are binary safe, and the
/// oracle test compares replies byte for byte.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// `+OK\r\n`
    Simple(Bytes),
    /// `-ERR message\r\n`
    Error(Bytes),
    /// `:42\r\n`
    Integer(i64),
    /// `$3\r\nfoo\r\n`
    Bulk(Bytes),
    /// `$-1\r\n`
    Null,
    /// `*-1\r\n`
    NullArray,
    /// `*2\r\n...`
    Array(Vec<Frame>),
}

impl Frame {
    /// `+OK`
    pub fn ok() -> Frame {
        Frame::Simple(Bytes::from_static(b"OK"))
    }

    /// Simple string.
    pub fn simple(s: impl Into<Bytes>) -> Frame {
        Frame::Simple(s.into())
    }

    /// Error reply; `s` includes the prefix, e.g. `"ERR syntax error"`.
    pub fn error(s: impl Into<Bytes>) -> Frame {
        Frame::Error(s.into())
    }

    /// Bulk string.
    pub fn bulk(s: impl Into<Bytes>) -> Frame {
        Frame::Bulk(s.into())
    }

    /// Request-shaped array: `["SET", "k", "v"]` becomes an array of bulk strings.
    pub fn command<I, T>(args: I) -> Frame
    where
        I: IntoIterator<Item = T>,
        T: AsRef<[u8]>,
    {
        Frame::Array(args.into_iter().map(|a| Frame::Bulk(Bytes::copy_from_slice(a.as_ref()))).collect())
    }

    /// Append the wire encoding to `out`.
    ///
    /// CR and LF inside simple strings and errors are replaced with a space
    /// (Redis does the same) so a reply can never inject extra frames.
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Frame::Simple(s) => {
                out.push(b'+');
                push_line(out, s);
                out.extend_from_slice(b"\r\n");
            }
            Frame::Error(s) => {
                out.push(b'-');
                push_line(out, s);
                out.extend_from_slice(b"\r\n");
            }
            Frame::Integer(n) => {
                out.push(b':');
                push_i64(out, *n);
                out.extend_from_slice(b"\r\n");
            }
            Frame::Bulk(b) => {
                out.push(b'$');
                push_u64(out, b.len() as u64);
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(b);
                out.extend_from_slice(b"\r\n");
            }
            Frame::Null => out.extend_from_slice(b"$-1\r\n"),
            Frame::NullArray => out.extend_from_slice(b"*-1\r\n"),
            Frame::Array(items) => {
                out.push(b'*');
                push_u64(out, items.len() as u64);
                out.extend_from_slice(b"\r\n");
                for item in items {
                    item.encode(out);
                }
            }
        }
    }

    /// Encode into a fresh buffer.
    pub fn to_vec(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode(&mut out);
        out
    }

    /// Decode one frame from the front of `buf`.
    ///
    /// * `Ok(Some((frame, n)))` - a frame occupying the first `n` bytes
    /// * `Ok(None)` - incomplete, call again with more bytes
    /// * `Err(_)` - the bytes can never become a valid frame
    pub fn parse(buf: &[u8]) -> Result<Option<(Frame, usize)>, ProtocolError> {
        parse_frame(buf, 0)
    }
}

/// A violation of the RESP2 grammar (or of our limits).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    /// `*N` where N is not a valid length.
    InvalidMultibulkLen,
    /// `$N` where N is not a valid length.
    InvalidBulkLen,
    /// A request element did not start with `$`.
    ExpectedDollar(u8),
    /// `*N` header line longer than [`MAX_LINE_LEN`].
    MbulkCountTooBig,
    /// `$N` header line longer than [`MAX_LINE_LEN`].
    BulkCountTooBig,
    /// A `+`/`-`/`:` line longer than [`MAX_LINE_LEN`].
    LineTooLong,
    /// Bulk payload not followed by CRLF.
    BadBulkTerminator,
    /// A `\r` not followed by `\n` inside a header line.
    BadLineEnding,
    /// `:` line that is not an integer.
    InvalidInteger,
    /// Unknown leading type byte (general decoder only).
    UnknownType(u8),
    /// Array nesting beyond [`MAX_NESTING`] (general decoder only).
    NestingTooDeep,
    /// Request did not start with `*` (inline commands are out of scope).
    InlineNotSupported,
}

impl ProtocolError {
    /// Text of the error reply (without the leading `ERR `). Wording follows
    /// Redis where Redis has an equivalent.
    pub fn message(&self) -> String {
        match self {
            ProtocolError::InvalidMultibulkLen => "Protocol error: invalid multibulk length".into(),
            ProtocolError::InvalidBulkLen => "Protocol error: invalid bulk length".into(),
            ProtocolError::ExpectedDollar(c) => {
                format!("Protocol error: expected '$', got '{}'", *c as char)
            }
            ProtocolError::MbulkCountTooBig => "Protocol error: too big mbulk count string".into(),
            ProtocolError::BulkCountTooBig => "Protocol error: too big bulk count string".into(),
            ProtocolError::LineTooLong => "Protocol error: line too long".into(),
            ProtocolError::BadBulkTerminator => "Protocol error: bulk string not terminated by CRLF".into(),
            ProtocolError::BadLineEnding => "Protocol error: invalid line ending".into(),
            ProtocolError::InvalidInteger => "Protocol error: invalid integer".into(),
            ProtocolError::UnknownType(c) => {
                format!("Protocol error: unknown type byte '{}'", *c as char)
            }
            ProtocolError::NestingTooDeep => "Protocol error: nesting too deep".into(),
            ProtocolError::InlineNotSupported => {
                "Protocol error: inline commands are not supported (send RESP arrays)".into()
            }
        }
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message())
    }
}

impl std::error::Error for ProtocolError {}

/// Strict decimal parse with exactly the semantics of Redis's `string2ll`:
/// optional leading `-`, first digit 1-9 (so no leading zeros, no `-0`),
/// no `+`, no whitespace, must fit in `i64`.
pub fn parse_i64(s: &[u8]) -> Option<i64> {
    if s.is_empty() || s.len() > 20 {
        return None;
    }
    if s == b"0" {
        return Some(0);
    }
    let (negative, digits) = match s[0] {
        b'-' => (true, &s[1..]),
        _ => (false, s),
    };
    match digits.first() {
        Some(b'1'..=b'9') => {}
        _ => return None,
    }
    let mut v: u64 = 0;
    for &d in digits {
        if !d.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(u64::from(d - b'0'))?;
    }
    if negative {
        if v > (i64::MAX as u64) + 1 {
            return None;
        }
        Some((v as i64).wrapping_neg())
    } else {
        i64::try_from(v).ok()
    }
}

fn push_line(out: &mut Vec<u8>, s: &[u8]) {
    out.extend(s.iter().map(|&b| if b == b'\r' || b == b'\n' { b' ' } else { b }));
}

fn push_u64(out: &mut Vec<u8>, mut n: u64) {
    let mut tmp = [0u8; 20];
    let mut i = tmp.len();
    loop {
        i -= 1;
        tmp[i] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    out.extend_from_slice(&tmp[i..]);
}

fn push_i64(out: &mut Vec<u8>, n: i64) {
    if n < 0 {
        out.push(b'-');
    }
    push_u64(out, n.unsigned_abs());
}

/// Find the CRLF-terminated line at the start of `buf`.
///
/// Returns `(line_len, total_len_including_crlf)`; `Ok(None)` if the line is not
/// complete yet; `Err(too_big)` if no CR shows up within [`MAX_LINE_LEN`] bytes.
fn find_line(buf: &[u8], too_big: ProtocolError) -> Result<Option<(usize, usize)>, ProtocolError> {
    let window = buf.len().min(MAX_LINE_LEN);
    match buf[..window].iter().position(|&b| b == b'\r') {
        Some(i) => match buf.get(i + 1) {
            Some(b'\n') => Ok(Some((i, i + 2))),
            Some(_) => Err(ProtocolError::BadLineEnding),
            None => Ok(None),
        },
        None if buf.len() >= MAX_LINE_LEN => Err(too_big),
        None => Ok(None),
    }
}

fn parse_frame(buf: &[u8], depth: usize) -> Result<Option<(Frame, usize)>, ProtocolError> {
    let Some(&kind) = buf.first() else {
        return Ok(None);
    };
    let body = &buf[1..];
    match kind {
        b'+' | b'-' => {
            let Some((len, total)) = find_line(body, ProtocolError::LineTooLong)? else {
                return Ok(None);
            };
            let s = Bytes::copy_from_slice(&body[..len]);
            let frame = if kind == b'+' { Frame::Simple(s) } else { Frame::Error(s) };
            Ok(Some((frame, 1 + total)))
        }
        b':' => {
            let Some((len, total)) = find_line(body, ProtocolError::LineTooLong)? else {
                return Ok(None);
            };
            let n = parse_i64(&body[..len]).ok_or(ProtocolError::InvalidInteger)?;
            Ok(Some((Frame::Integer(n), 1 + total)))
        }
        b'$' => {
            let Some((len, total)) = find_line(body, ProtocolError::BulkCountTooBig)? else {
                return Ok(None);
            };
            let n = parse_i64(&body[..len]).ok_or(ProtocolError::InvalidBulkLen)?;
            if n == -1 {
                return Ok(Some((Frame::Null, 1 + total)));
            }
            if n < 0 || n as u64 > MAX_BULK_LEN as u64 {
                return Err(ProtocolError::InvalidBulkLen);
            }
            let start = 1 + total;
            let end = start + n as usize;
            if buf.len() < end + 2 {
                return Ok(None);
            }
            if &buf[end..end + 2] != b"\r\n" {
                return Err(ProtocolError::BadBulkTerminator);
            }
            Ok(Some((Frame::Bulk(Bytes::copy_from_slice(&buf[start..end])), end + 2)))
        }
        b'*' => {
            let Some((len, total)) = find_line(body, ProtocolError::MbulkCountTooBig)? else {
                return Ok(None);
            };
            let n = parse_i64(&body[..len]).ok_or(ProtocolError::InvalidMultibulkLen)?;
            if n == -1 {
                return Ok(Some((Frame::NullArray, 1 + total)));
            }
            if n < 0 || n > i64::from(i32::MAX) {
                return Err(ProtocolError::InvalidMultibulkLen);
            }
            if depth >= MAX_NESTING {
                return Err(ProtocolError::NestingTooDeep);
            }
            let n = n as usize;
            // Never trust a header for allocation size: grow as elements really arrive.
            let mut items = Vec::with_capacity(n.min(1024));
            let mut pos = 1 + total;
            for _ in 0..n {
                match parse_frame(&buf[pos..], depth + 1)? {
                    Some((frame, used)) => {
                        items.push(frame);
                        pos += used;
                    }
                    None => return Ok(None),
                }
            }
            Ok(Some((Frame::Array(items), pos)))
        }
        other => Err(ProtocolError::UnknownType(other)),
    }
}

/// A decoded request: its arguments (borrowed from the input buffer) and the
/// number of input bytes it occupied.
pub type ParsedRequest<'a> = (Vec<&'a [u8]>, usize);

/// Decode one *request* (a flat array of bulk strings) from the front of `buf`.
///
/// The returned arguments borrow from `buf`. An empty vector means the client
/// sent `*0\r\n` or `*-1\r\n`, which Redis skips without replying.
///
/// Any first byte other than `*` is an inline command and is rejected
/// ([`ProtocolError::InlineNotSupported`]).
pub fn parse_request(buf: &[u8]) -> Result<Option<ParsedRequest<'_>>, ProtocolError> {
    let Some(&first) = buf.first() else {
        return Ok(None);
    };
    if first != b'*' {
        return Err(ProtocolError::InlineNotSupported);
    }
    let Some((len, total)) = find_line(&buf[1..], ProtocolError::MbulkCountTooBig)? else {
        return Ok(None);
    };
    let n = parse_i64(&buf[1..1 + len]).ok_or(ProtocolError::InvalidMultibulkLen)?;
    let mut pos = 1 + total;
    if n <= 0 {
        return Ok(Some((Vec::new(), pos)));
    }
    if n as u64 > MAX_REQUEST_ARGS as u64 {
        return Err(ProtocolError::InvalidMultibulkLen);
    }
    let n = n as usize;
    let mut args: Vec<&[u8]> = Vec::with_capacity(n.min(64));
    for _ in 0..n {
        let Some(&c) = buf.get(pos) else {
            return Ok(None);
        };
        if c != b'$' {
            return Err(ProtocolError::ExpectedDollar(c));
        }
        let Some((len, total)) = find_line(&buf[pos + 1..], ProtocolError::BulkCountTooBig)? else {
            return Ok(None);
        };
        let blen = parse_i64(&buf[pos + 1..pos + 1 + len]).ok_or(ProtocolError::InvalidBulkLen)?;
        if blen < 0 || blen as u64 > MAX_BULK_LEN as u64 {
            return Err(ProtocolError::InvalidBulkLen);
        }
        let start = pos + 1 + total;
        let end = start + blen as usize;
        if buf.len() < end + 2 {
            return Ok(None);
        }
        if buf[end] != b'\r' || buf[end + 1] != b'\n' {
            return Err(ProtocolError::BadBulkTerminator);
        }
        args.push(&buf[start..end]);
        pos = end + 2;
    }
    Ok(Some((args, pos)))
}

/// Encode a request (`*N` of bulk strings) - what a client sends and what the AOF stores.
pub fn encode_request(args: &[&[u8]], out: &mut Vec<u8>) {
    out.push(b'*');
    push_u64(out, args.len() as u64);
    out.extend_from_slice(b"\r\n");
    for a in args {
        out.push(b'$');
        push_u64(out, a.len() as u64);
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_i64_matches_string2ll() {
        assert_eq!(parse_i64(b"0"), Some(0));
        assert_eq!(parse_i64(b"-5"), Some(-5));
        assert_eq!(parse_i64(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(parse_i64(b"-9223372036854775808"), Some(i64::MIN));
        for bad in [
            &b""[..],
            b"-",
            b"-0",
            b"+5",
            b" 5",
            b"5 ",
            b"007",
            b"1.5",
            b"abc",
            b"9223372036854775808",
            b"-9223372036854775809",
            b"99999999999999999999",
            b"123456789012345678901",
        ] {
            assert_eq!(parse_i64(bad), None, "{:?}", String::from_utf8_lossy(bad));
        }
    }

    #[test]
    fn integer_encoding_extremes() {
        let mut out = Vec::new();
        Frame::Integer(i64::MIN).encode(&mut out);
        assert_eq!(out, b":-9223372036854775808\r\n");
        out.clear();
        Frame::Integer(0).encode(&mut out);
        assert_eq!(out, b":0\r\n");
    }
}
