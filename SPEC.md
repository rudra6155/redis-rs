# SPEC — redis-rs

Every statement here is either enforced by a test (named in the right-hand column
of the tables) or listed as a known divergence. Oracle = `redis:7` (Docker),
measured version **7.4.12** (`INFO server` → `redis_version`; also recorded in
`results/oracle_report.txt`). Statements about what Redis does *on the wire* are
oracle-checked; remarks about Redis *internals* (its active-expire rule,
behaviour while loading an AOF) are from memory of its source and not verified
here.

## 1. Scope

| Item | Decision |
|---|---|
| Wire protocol | **RESP2 only.** No `HELLO`, no RESP3 types (maps, sets, doubles, push…). |
| Commands | `PING` `ECHO` `SET` (`EX`/`PX`) `GET` `DEL` `EXISTS` `TTL` `EXPIRE` `INCR` `HSET` `HGET` `HDEL` `HLEN` `HGETALL` |
| Value types | string, hash |
| Persistence | **AOF only.** No RDB, no `BGREWRITEAOF` (the file grows without bound — documented limitation). |
| Concurrency model | (b) tokio multi-threaded runtime + 64 hash-sharded stores, one `std::sync::Mutex` per shard. Defended in `ARCHITECTURE.md`. |
| Platform | Linux is the tested platform (signals, `kill -9`, Docker). The crate also compiles elsewhere (`cfg(unix)` gates), but non-Linux is untested. |

### Out of scope (so nobody files them as bugs)

RESP3, pub/sub, `MULTI`/`EXEC`/`WATCH`, `SCAN`/`KEYS`/`DBSIZE`, replication,
cluster, ACL/`AUTH`, `SELECT` (single keyspace), scripting (`EVAL`/functions),
`WAIT`, `BITFIELD`/bit ops, streams, `CONFIG`, `INFO`, `CLIENT`, `COMMAND`,
`QUIT`, `FLUSHALL`, `MONITOR`, blocking commands, RDB, `BGREWRITEAOF`, TLS,
`maxmemory`/eviction, keyspace notifications.
All of these answer `ERR unknown command …` exactly like an unknown command.

## 2. RESP2 wire contract

* Parses and emits: simple string `+`, error `-`, integer `:`, bulk string `$`,
  array `*`, null bulk `$-1\r\n`, null array `*-1\r\n`. (tests: `resp_unit`)
* Bulk strings are **length-prefixed**, never delimiter-scanned, so payloads may
  contain `\r\n` and arbitrary bytes. (`resp_unit`, `protocol_oracle`)
* Partial reads (a command split across TCP segments) and pipelining (many
  commands in one segment) are handled by the incremental parser.
  (`resp_unit`: byte-by-byte feed + real TCP segments; `protocol_oracle`)
* **Inline commands are out of scope.** A request whose first byte is not `*`
  gets `-ERR Protocol error: inline commands are not supported (send RESP arrays)`
  and the connection is closed (after an inline line the stream cannot be
  re-synchronised). Real Redis would execute `PING\r\n`. *Divergence D1.*
* Requests of `*0\r\n` / `*-1\r\n` are silently skipped (same as Redis).
* Protocol-error wording is copied from Redis and diffed by the oracle test:
  `invalid multibulk length`, `invalid bulk length`, `expected '$', got 'x'`.
  Redis closes the connection after a protocol error; so do we.
* Limits: bulk ≤ 512 MiB (Redis `proto-max-bulk-len` default), request args ≤
  1 048 576 (Redis 7.4 allows up to `INT_MAX`; *Divergence D2*), header line ≤
  64 KiB, query buffer ≤ 1 GiB (connection closed beyond that).
* Strictness: a bulk payload must be followed by exactly `\r\n`. Redis skips
  those 2 bytes without looking. We reject with
  `ERR Protocol error: bulk string not terminated by CRLF`. *Divergence D3* —
  deliberate (detects desynchronised clients instead of silently misparsing).

## 3. Error semantics (exact bytes, diffed against the oracle)

| Case | Reply | Test |
|---|---|---|
| Unknown command `FOO` | `-ERR unknown command 'FOO', with args beginning with: ` (+ `'arg' ` per arg, 128-byte budget, command name as typed) | `error_semantics`, `protocol_oracle` |
| Arity `GET` | `-ERR wrong number of arguments for 'get' command` (lower-case name) | same |
| Wrong type | `-WRONGTYPE Operation against a key holding the wrong kind of value` | same |
| `INCR` on non-integer | `-ERR value is not an integer or out of range` | same |
| `INCR` overflow | `-ERR increment or decrement would overflow` | same |
| Bad expire value | `-ERR invalid expire time in 'set' command` / `'expire'` | same |
| Bad option | `-ERR syntax error` | same |

Integer parsing is Redis's `string2ll`: optional `-`, no `+`, no leading zeros,
no spaces, `-0` invalid, must fit `i64`. So `INCR` on `007`, `+5`, ` 5`, `-0`
is an error exactly like Redis (all in the oracle script).

## 4. Command semantics

| Command | Behaviour | Notes |
|---|---|---|
| `PING [msg]` | `+PONG`, or bulk `msg` | |
| `ECHO msg` | bulk `msg` | |
| `SET k v [EX s \| PX ms]` | `+OK`; value and deadline set atomically (one shard-lock scope); **clears any existing TTL** when no `EX`/`PX`; overwrites any type | `NX XX GET KEEPTTL EXAT PXAT` → `ERR syntax error` (*D4*). Like Redis: `EX`+`PX` together → syntax error; repeated `EX` → last wins. |
| `GET k` | bulk, or `$-1` if missing/expired; `WRONGTYPE` on hash | |
| `DEL k` | `:1` if a live key was removed, else `:0` | **single key only** (*D5*): `DEL a b` → arity error (Redis deletes both). |
| `EXISTS k` | `:1` / `:0` | **single key only** (*D5*). |
| `TTL k` | `-2` missing/expired, `-1` no TTL, else remaining seconds | **Rounded to nearest second, like Redis (`(ms+500)/1000`)** — see §4.1. |
| `EXPIRE k s` | `:1` set (replaces an existing TTL), `:0` key missing; `s ≤ 0` deletes the key and replies `:1` | no `NX/XX/GT/LT` options (*D6*): extra args → arity error. Argument validation happens before the key lookup, as in Redis. |
| `INCR k` | creates `1` if missing; errors on non-integer / `i64` overflow; **preserves TTL** | |
| `HSET k f v [f v …]` | number of **new** fields (updates don't count); creates the hash; preserves TTL | odd number of f/v args → arity error |
| `HGET k f` | bulk or `$-1` (field or key missing) | |
| `HDEL k f [f …]` | number of fields removed; **removing the last field deletes the key** | |
| `HLEN k` | integer (`0` if missing) | |
| `HGETALL k` | flat array `[f, v, f, v …]` in **insertion order**, empty array if missing | Redis only guarantees this order for small hashes (listpack encoding, ≤512 fields and ≤64-byte values); the oracle script stays inside that regime. |
| any hash command on a string key (and `GET`/`INCR` on a hash) | `WRONGTYPE` | both directions tested |

### 4.1 TTL rounding

An earlier draft specified `TTL` as "truncated toward zero". The oracle says otherwise:
against `redis:7.4.12`, `SET k v EX 100` followed immediately by `TTL k` returns
**100** (and `PTTL k` returned 99999), i.e. Redis computes
`(remaining_ms + 500) / 1000`. A truncating implementation would answer 99 and
visibly disagree with Redis in the first demo anyone runs. Since the whole point
is to match real Redis, **we follow the oracle** (round to nearest). It is a one-line
change in `Shard::ttl` (`src/store.rs`) if truncation is wanted instead; the
`expiry` tests assert the rounded semantics (`TTL` of a fresh `EX 100` key is
exactly 100).

## 5. Expiry contract

* **Lazy**: every command that touches a key checks its deadline first
  (`now > deadline`, same comparison as Redis) and deletes it if expired.
* **Sweeper**: a dedicated thread wakes every **100 ms**; for each of the 64
  shards it takes the shard lock for one *round* of ≤ 20 random samples from
  that shard's TTL-key list, deletes the expired ones, and repeats (≤ 16
  rounds per shard per tick) only while > 25 % of the sample was expired —
  Redis's adaptive active-expire rule. Upper bound: 64 × 16 × 20 = 20 480
  samples per tick.
* **Tradeoff**: sampling is probabilistic, so a key can stay *physically
  present but logically expired* for several ticks (worse when a shard holds
  millions of TTL keys and few are expired). It is never *visible* to a command
  that touches it (lazy expiry). Anything that counts physical keys without
  touching them would over-count; this server has no `DBSIZE`/`KEYS`/`SCAN`, so
  nothing user-visible can observe it. Tests observe it through the library
  (`Store::raw_len`).
* **Expiry is journaled.** Whenever a key is deleted *because it expired*
  (lazily or by the sweeper) a `DEL` is appended to the AOF (as real Redis does).
  Without it, replay could resurrect a key that expired at runtime and then
  apply a later `INCR` to the stale value (see README "Things that broke").
* Tests: `tests/expiry.rs` — `PX 50` key unreadable after 150 ms **and
  physically gone** with the sweeper on, without any command touching it; lazy
  expiry correct with the sweeper disabled; sweeper reaps within 500 ms of the
  deadline.

## 6. Persistence contract (AOF)

* Format = Redis's: each write is a RESP array of bulk strings
  (`*3\r\n$3\r\nSET\r\n…`). Format cross-checked against a real Redis AOF
  (`results/real_redis_aof_sample.txt`).
* The log records the *effect*, not the request: `SET k v EX 10` is logged as
  `SET k v PXAT <absolute-unix-ms>`, `EXPIRE k s` as `PEXPIREAT k <absolute>`
  (or `DEL k` if already due), expiry-deletions as `DEL k`. `PXAT` and
  `PEXPIREAT` are accepted **only while replaying the AOF**; from a network
  client they are `unknown command`/`syntax error` (they are not in scope).
* `--appendfsync always|everysec|no` (default `everysec`):
  * `always` — a single AOF writer thread group-commits: it drains every queued
    record, `write(2)`s them, `fdatasync`s once, then releases the clients whose
    batch is included. A reply is never sent before its record is on disk.
  * `everysec` — the writer thread `write(2)`s immediately; a **separate
    background thread** `fdatasync`s once per second. Replies don't wait.
  * `no` — `write(2)` only; the OS decides when to flush. A final `fsync` runs at
    graceful shutdown.
* **Crash window** (what can be lost): `always`: nothing acknowledged (process
  or OS crash). `everysec`: ≤ ~1 s of acknowledged writes on **OS crash/power
  loss**; on a plain process kill (`kill -9`) only the records still in the
  in-process queue (microseconds–milliseconds). `no`: whatever the kernel hasn't
  flushed on OS crash (typically ≤ 30 s); process kill behaves like `everysec`.
* **Replay on startup** (before the listener is bound): read the AOF, execute in
  order, then serve. While replaying, *no key is considered expired* (Redis does
  the same while loading) so replay is a pure function of the log; deadlines
  are absolute so a key whose deadline passed during downtime is gone at its
  first access / first sweep.
* **Truncated tail policy = (b) truncate-and-start.** If the file ends in the
  middle of a command (the torn write of a crash) the incomplete tail is
  removed (`ftruncate` to the last good offset, fsynced), the server logs what it
  dropped to stderr **and** writes `<aof>.corruption-report.txt`
  (`{action, bytes_dropped, last_good_offset, offending_hex}`), then starts.
  **Any other corruption** — garbage bytes mid-file, a record that fails to
  re-execute — is *not* guessed at: the server refuses to start (exit 1) with
  the offset and writes the same report with `action: refused_to_start`.
* Graceful shutdown (`SIGINT`/`SIGTERM`): stop accepting, let each connection
  finish the commands it already read, close, drain the AOF queue, `fsync`, exit
  **0**. A second signal forces an immediate exit 1.
* An I/O error while writing/fsyncing the AOF is **fatal** (process exits 1):
  continuing would acknowledge writes that are not durable.

## 7. Concurrency contract

See `ARCHITECTURE.md` for the defence. Hard rules (enforced):

1. No `std::sync` guard is ever held across an `.await`. Enforced three ways:
   (i) shard locks only exist inside the closure of `Store::locked`, which is a
   synchronous function; (ii) command handlers are synchronous `fn`s;
   (iii) `clippy::await_holding_lock` is `deny` in `Cargo.toml` `[lints]`, in
   `lib.rs`, and CI runs `cargo clippy --all-targets -- -D warnings`. A
   negative control (a deliberately bad file) is in `results/mutation_report.txt`.
2. A command handler holds **at most one shard lock**, never two, because every
   command in scope touches exactly one key. The only lock ever taken *while*
   holding a shard lock is the AOF channel's internal queue lock (order
   `shard → queue`, never reversed; the AOF writer never touches a shard).
3. The sweeper takes one shard lock at a time.

## 8. Known divergences from Redis 7.4 (complete list)

| # | Divergence | Why |
|---|---|---|
| D1 | Inline commands rejected | scope |
| D2 | max 1 048 576 args per request | cheap DoS bound |
| D3 | bulk payload must be followed by `\r\n` | detects desync |
| D4 | `SET` options `NX XX GET KEEPTTL EXAT PXAT` unsupported (`syntax error`) | scope |
| D5 | `DEL`/`EXISTS` single key only (arity error for 2+) | scope |
| D6 | `EXPIRE` has no `NX/XX/GT/LT` | scope |
| D7 | commands outside the list answer *unknown command* | scope |
| D8 | `TTL` rounds like Redis, initial spec said truncate | see §4.1 (this is **not** a divergence from Redis, only from the initial spec) |

## 9. Non-goals / honest limits

* Not a drop-in Redis replacement: "compatible" here means *byte-identical
  replies for the in-scope command set and the error cases above*, as proven by
  `results/oracle_report.txt` for the version noted at the top.
* Benchmarks compare against a single-threaded real Redis on the same machine;
  the comparison caveats are in the README.
