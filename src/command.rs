//! Command table, argument validation and handlers.
//!
//! Handlers are plain synchronous functions `fn(&mut Ctx, &[&[u8]]) -> Frame`:
//! they cannot `.await`, so they cannot hold a lock across one. All data access
//! goes through [`Ctx::shard`], which takes exactly one shard lock for the
//! duration of the closure.
//!
//! Error strings are copied from Redis 7.4 and diffed against a real
//! `redis-server` by `tests/protocol_oracle.rs`; do not reword them.

use crate::expiry::now_ms;
use crate::resp::{parse_i64, Frame};
use crate::store::{Shard, Store, StoreError};
use bytes::Bytes;

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
const NOT_INTEGER: &str = "ERR value is not an integer or out of range";
const OVERFLOW: &str = "ERR increment or decrement would overflow";
const SYNTAX: &str = "ERR syntax error";

/// Execution context for one command (or one pipelined batch of them).
pub struct Ctx<'a> {
    store: &'a Store,
    /// Clock reading for the command being executed (unix ms). 0 while replaying:
    /// nothing is considered expired during AOF load (as in Redis), so replay is
    /// a pure function of the log.
    now: u64,
    replay: bool,
    wrote: bool,
}

impl<'a> Ctx<'a> {
    /// Context for live client commands.
    pub fn new(store: &'a Store) -> Ctx<'a> {
        Ctx { store, now: 0, replay: false, wrote: false }
    }

    /// Context for AOF replay: accepts the internal `PXAT`/`PEXPIREAT` forms and
    /// never expires anything.
    pub fn replay(store: &'a Store) -> Ctx<'a> {
        Ctx { store, now: 0, replay: true, wrote: false }
    }

    /// Did any command since the last call queue an AOF record? Resets the flag.
    /// The connection uses this to decide whether to wait for the AOF (`always`).
    pub fn take_wrote(&mut self) -> bool {
        std::mem::take(&mut self.wrote)
    }

    /// Run `f` under the lock of the shard that owns `key`.
    fn shard<R>(&mut self, key: &[u8], f: impl FnOnce(&mut Shard, u64) -> R) -> R {
        let now = self.now;
        let (result, wrote) = self.store.locked(key, |shard| {
            let result = f(shard, now);
            (result, shard.take_wrote())
        });
        self.wrote |= wrote;
        result
    }
}

type Handler = fn(&mut Ctx<'_>, &[&[u8]]) -> Frame;

struct CommandSpec {
    /// Lower-case name, as Redis prints it in arity errors.
    name: &'static str,
    /// Redis convention: `n > 0` exactly n args including the name; `n < 0` at least `-n`.
    arity: i32,
    /// Only accepted while replaying the AOF; from a client it is "unknown command".
    internal: bool,
    handler: Handler,
}

/// The command table. Adding a command = one handler + one row here
/// (ARCHITECTURE.md, "How to add a new command").
static COMMANDS: &[CommandSpec] = &[
    CommandSpec { name: "ping", arity: -1, internal: false, handler: cmd_ping },
    CommandSpec { name: "echo", arity: 2, internal: false, handler: cmd_echo },
    CommandSpec { name: "set", arity: -3, internal: false, handler: cmd_set },
    CommandSpec { name: "get", arity: 2, internal: false, handler: cmd_get },
    CommandSpec { name: "del", arity: 2, internal: false, handler: cmd_del },
    CommandSpec { name: "exists", arity: 2, internal: false, handler: cmd_exists },
    CommandSpec { name: "ttl", arity: 2, internal: false, handler: cmd_ttl },
    CommandSpec { name: "expire", arity: 3, internal: false, handler: cmd_expire },
    CommandSpec { name: "incr", arity: 2, internal: false, handler: cmd_incr },
    CommandSpec { name: "hset", arity: -4, internal: false, handler: cmd_hset },
    CommandSpec { name: "hget", arity: 3, internal: false, handler: cmd_hget },
    CommandSpec { name: "hdel", arity: -3, internal: false, handler: cmd_hdel },
    CommandSpec { name: "hlen", arity: 2, internal: false, handler: cmd_hlen },
    CommandSpec { name: "hgetall", arity: 2, internal: false, handler: cmd_hgetall },
    // AOF-only: how EXPIRE is journaled (absolute deadline).
    CommandSpec { name: "pexpireat", arity: 3, internal: true, handler: cmd_pexpireat },
];

fn lookup(name: &[u8]) -> Option<&'static CommandSpec> {
    COMMANDS.iter().find(|c| c.name.as_bytes().eq_ignore_ascii_case(name))
}

/// Execute one request (`args[0]` is the command name) and produce its reply.
pub fn execute(ctx: &mut Ctx<'_>, args: &[&[u8]]) -> Frame {
    let Some(name) = args.first() else {
        return Frame::error("ERR empty command");
    };
    let Some(spec) = lookup(name) else {
        return unknown_command(args);
    };
    if spec.internal && !ctx.replay {
        return unknown_command(args);
    }
    let arity_ok = if spec.arity >= 0 {
        args.len() == spec.arity as usize
    } else {
        args.len() >= spec.arity.unsigned_abs() as usize
    };
    if !arity_ok {
        return arity_error(spec.name);
    }
    ctx.now = if ctx.replay { 0 } else { now_ms() };
    (spec.handler)(ctx, args)
}

// ---- error helpers ----

fn arity_error(name: &str) -> Frame {
    Frame::error(format!("ERR wrong number of arguments for '{name}' command"))
}

fn syntax_error() -> Frame {
    Frame::error(SYNTAX)
}

fn not_integer() -> Frame {
    Frame::error(NOT_INTEGER)
}

fn invalid_expire(cmd: &str) -> Frame {
    Frame::error(format!("ERR invalid expire time in '{cmd}' command"))
}

fn store_error(e: StoreError) -> Frame {
    Frame::error(match e {
        StoreError::WrongType => WRONGTYPE,
        StoreError::NotInteger => NOT_INTEGER,
        StoreError::Overflow => OVERFLOW,
    })
}

/// `s` up to its first NUL and at most `max` bytes: what C's `%.*s` prints.
fn c_prefix(s: &[u8], max: usize) -> &[u8] {
    let end = s.iter().position(|&b| b == 0).unwrap_or(s.len());
    &s[..end.min(max)]
}

/// Redis 7: `unknown command 'NAME', with args beginning with: 'a1' 'a2' ` where
/// the name is cut at 128 bytes and the argument list gets a 128-byte budget.
fn unknown_command(args: &[&[u8]]) -> Frame {
    let mut msg = b"ERR unknown command '".to_vec();
    msg.extend_from_slice(c_prefix(args[0], 128));
    msg.extend_from_slice(b"', with args beginning with: ");
    let mut listed: Vec<u8> = Vec::new();
    for arg in &args[1..] {
        if listed.len() >= 128 {
            break;
        }
        let room = 128 - listed.len();
        listed.push(b'\'');
        listed.extend_from_slice(c_prefix(arg, room));
        listed.extend_from_slice(b"' ");
    }
    msg.extend_from_slice(&listed);
    Frame::error(msg)
}

fn copy(b: &[u8]) -> Bytes {
    Bytes::copy_from_slice(b)
}

// ---- handlers ----

fn cmd_ping(_ctx: &mut Ctx<'_>, args: &[&[u8]]) -> Frame {
    match args.len() {
        1 => Frame::simple("PONG"),
        2 => Frame::Bulk(copy(args[1])),
        _ => arity_error("ping"),
    }
}

fn cmd_echo(_ctx: &mut Ctx<'_>, args: &[&[u8]]) -> Frame {
    Frame::Bulk(copy(args[1]))
}

/// `SET key value [EX seconds | PX milliseconds]`.
///
/// Option parsing mirrors Redis: `EX`+`PX` together is a syntax error, a repeated
/// `EX` (or `PX`) is allowed and the last one wins, the numeric argument is only
/// validated once the whole option list parsed. While replaying, `PXAT <abs ms>`
/// is also accepted (that is how the AOF stores deadlines).
fn cmd_set(ctx: &mut Ctx<'_>, args: &[&[u8]]) -> Frame {
    #[derive(Clone, Copy)]
    enum Opt<'a> {
        Ex(&'a [u8]),
        Px(&'a [u8]),
        PxAt(&'a [u8]),
    }

    let (key, value) = (args[1], args[2]);
    let mut opt: Option<Opt<'_>> = None;
    let mut i = 3;
    while i < args.len() {
        let name = args[i];
        let Some(&arg) = args.get(i + 1) else {
            return syntax_error();
        };
        opt = if name.eq_ignore_ascii_case(b"EX") && matches!(opt, None | Some(Opt::Ex(_))) {
            Some(Opt::Ex(arg))
        } else if name.eq_ignore_ascii_case(b"PX") && matches!(opt, None | Some(Opt::Px(_))) {
            Some(Opt::Px(arg))
        } else if ctx.replay && name.eq_ignore_ascii_case(b"PXAT") && matches!(opt, None | Some(Opt::PxAt(_))) {
            Some(Opt::PxAt(arg))
        } else {
            return syntax_error();
        };
        i += 2;
    }

    let expires_at = match opt {
        None => None,
        Some(Opt::PxAt(v)) => match parse_i64(v) {
            Some(t) if t > 0 => Some(t as u64),
            Some(_) => return invalid_expire("set"),
            None => return not_integer(),
        },
        Some(Opt::Ex(v) | Opt::Px(v)) => {
            if ctx.replay {
                return Frame::error("ERR relative expire time is not allowed in the AOF");
            }
            let Some(n) = parse_i64(v) else {
                return not_integer();
            };
            let seconds = matches!(opt, Some(Opt::Ex(_)));
            if n <= 0 || (seconds && n > i64::MAX / 1000) {
                return invalid_expire("set");
            }
            let ms = if seconds { n * 1000 } else { n };
            match ms.checked_add(ctx.now as i64) {
                Some(at) => Some(at as u64),
                None => return invalid_expire("set"),
            }
        }
    };

    ctx.shard(key, |sh, _| sh.set(key, value, expires_at));
    Frame::ok()
}

fn cmd_get(ctx: &mut Ctx<'_>, args: &[&[u8]]) -> Frame {
    let key = args[1];
    match ctx.shard(key, |sh, now| sh.get(key, now)) {
        Ok(Some(v)) => Frame::Bulk(v),
        Ok(None) => Frame::Null,
        Err(e) => store_error(e),
    }
}

fn cmd_del(ctx: &mut Ctx<'_>, args: &[&[u8]]) -> Frame {
    let key = args[1];
    Frame::Integer(i64::from(ctx.shard(key, |sh, now| sh.del(key, now))))
}

fn cmd_exists(ctx: &mut Ctx<'_>, args: &[&[u8]]) -> Frame {
    let key = args[1];
    Frame::Integer(i64::from(ctx.shard(key, |sh, now| sh.exists(key, now))))
}

fn cmd_ttl(ctx: &mut Ctx<'_>, args: &[&[u8]]) -> Frame {
    let key = args[1];
    Frame::Integer(ctx.shard(key, |sh, now| sh.ttl(key, now)))
}

/// `EXPIRE key seconds`. Validation order follows Redis: the number is parsed and
/// range-checked *before* the key is looked up, so `EXPIRE missing abc` is an error.
fn cmd_expire(ctx: &mut Ctx<'_>, args: &[&[u8]]) -> Frame {
    let key = args[1];
    let Some(seconds) = parse_i64(args[2]) else {
        return not_integer();
    };
    if !(i64::MIN / 1000..=i64::MAX / 1000).contains(&seconds) {
        return invalid_expire("expire");
    }
    let ms = seconds * 1000;
    let now = ctx.now as i64;
    if ms > i64::MAX - now {
        return invalid_expire("expire");
    }
    let at = ms + now;
    Frame::Integer(i64::from(ctx.shard(key, |sh, now| sh.expire_at(key, at, now))))
}

/// AOF-only `PEXPIREAT key absolute-ms`.
fn cmd_pexpireat(ctx: &mut Ctx<'_>, args: &[&[u8]]) -> Frame {
    let key = args[1];
    let Some(at) = parse_i64(args[2]) else {
        return not_integer();
    };
    Frame::Integer(i64::from(ctx.shard(key, |sh, now| sh.expire_at(key, at, now))))
}

fn cmd_incr(ctx: &mut Ctx<'_>, args: &[&[u8]]) -> Frame {
    let key = args[1];
    match ctx.shard(key, |sh, now| sh.incr(key, now)) {
        Ok(n) => Frame::Integer(n),
        Err(e) => store_error(e),
    }
}

fn cmd_hset(ctx: &mut Ctx<'_>, args: &[&[u8]]) -> Frame {
    // args = [HSET, key, f1, v1, ...]: the field/value tail must come in pairs.
    if !args.len().is_multiple_of(2) {
        return arity_error("hset");
    }
    let key = args[1];
    match ctx.shard(key, |sh, now| sh.hset(key, &args[2..], now)) {
        Ok(added) => Frame::Integer(added as i64),
        Err(e) => store_error(e),
    }
}

fn cmd_hget(ctx: &mut Ctx<'_>, args: &[&[u8]]) -> Frame {
    let (key, field) = (args[1], args[2]);
    match ctx.shard(key, |sh, now| sh.hget(key, field, now)) {
        Ok(Some(v)) => Frame::Bulk(v),
        Ok(None) => Frame::Null,
        Err(e) => store_error(e),
    }
}

fn cmd_hdel(ctx: &mut Ctx<'_>, args: &[&[u8]]) -> Frame {
    let key = args[1];
    match ctx.shard(key, |sh, now| sh.hdel(key, &args[2..], now)) {
        Ok(removed) => Frame::Integer(removed as i64),
        Err(e) => store_error(e),
    }
}

fn cmd_hlen(ctx: &mut Ctx<'_>, args: &[&[u8]]) -> Frame {
    let key = args[1];
    match ctx.shard(key, |sh, now| sh.hlen(key, now)) {
        Ok(n) => Frame::Integer(n as i64),
        Err(e) => store_error(e),
    }
}

fn cmd_hgetall(ctx: &mut Ctx<'_>, args: &[&[u8]]) -> Frame {
    let key = args[1];
    match ctx.shard(key, |sh, now| sh.hgetall(key, now)) {
        Ok(items) => Frame::Array(items.into_iter().map(Frame::Bulk).collect()),
        Err(e) => store_error(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(store: &Store, args: &[&str]) -> Frame {
        let bytes: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
        execute(&mut Ctx::new(store), &bytes)
    }

    fn err_text(f: &Frame) -> String {
        match f {
            Frame::Error(e) => String::from_utf8_lossy(e).into_owned(),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[test]
    fn unknown_command_text_matches_redis_format() {
        let store = Store::new(2);
        assert_eq!(err_text(&run(&store, &["FOO"])), "ERR unknown command 'FOO', with args beginning with: ");
        assert_eq!(
            err_text(&run(&store, &["foo", "1", "2"])),
            "ERR unknown command 'foo', with args beginning with: '1' '2' "
        );
        let long = "y".repeat(100);
        let text = err_text(&run(&store, &["FOO", &long, &long]));
        // second argument gets the 128 - 103 = 25 byte remainder, exactly like Redis
        assert!(text.ends_with(&format!("'{long}' '{}' ", "y".repeat(25))), "{text}");
    }

    #[test]
    fn internal_commands_are_invisible_to_clients() {
        let store = Store::new(2);
        assert!(err_text(&run(&store, &["PEXPIREAT", "k", "5"])).starts_with("ERR unknown command"));
        assert_eq!(err_text(&run(&store, &["SET", "k", "v", "PXAT", "5"])), SYNTAX);
    }

    #[test]
    fn set_get_roundtrip_and_arity() {
        let store = Store::new(2);
        assert_eq!(run(&store, &["SET", "k", "v"]), Frame::ok());
        assert_eq!(run(&store, &["GET", "k"]), Frame::Bulk(Bytes::from_static(b"v")));
        assert_eq!(err_text(&run(&store, &["GET"])), "ERR wrong number of arguments for 'get' command");
    }
}
