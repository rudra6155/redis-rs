# ARCHITECTURE

Read this top to bottom and you can rebuild the server on a whiteboard. Every claim
that has a test says which one; scope and exact semantics live in [SPEC.md](SPEC.md).

> **Provenance note.** What Redis does *on the wire* was checked against a real `redis:7` (7.4.12) by the
> oracle tests. Remarks below about how Redis works *inside* ("Redis does the same while loading", its
> adaptive active-expire rule, "Redis behaves the same under `always`") are from my memory of its source and
> were **not verified in this project**; none of the tests depend on them.

```
 src/resp.rs     RESP2 frames, encoder, two incremental decoders (general + request)
 src/store.rs    Store = 64 x Mutex<Shard>; Shard = HashMap + TTL index; ALL data access
 src/expiry.rs   wall clock + the 100 ms sweeper thread
 src/aof.rs      AOF writer thread, fsync policy, replay + torn-tail policy
 src/command.rs  command table, arity, argument validation, handlers (all synchronous)
 src/conn.rs     one connection: read -> process() -> [AOF barrier] -> write
 src/server.rs   accept loop, connection limit, graceful-shutdown order
 src/signal.rs   SIGINT/SIGTERM handlers (installed before "Ready"), StopSignal
 src/main.rs     flags, start-up order (replay BEFORE bind)
 src/lib.rs      (extra; not in the main tree) lets tests/ and benches/ link the real code
```

## 1. Connection lifecycle

One tokio task per connection. `conn::handle` loops
`process() -> barrier? -> write_all -> read_buf | shutdown`. `process()` is a plain
`fn`: it decodes every complete request in the buffer, runs it, and appends the
encoded replies to one output `Vec`. Because it is synchronous, nothing inside it
can `.await`, so nothing inside it can hold a lock across an await.

### GET k

```
 client            conn task (tokio worker)                  shard k%64 (std Mutex)
   |  *2 $3 GET $1 k    |                                          |
   |------------------->| read_buf -> inbuf                        |
   |                    | process():                               |
   |                    |   parse_request(&inbuf)  (borrows args, no copy)
   |                    |   execute(): table lookup "get", arity==2 ok, ctx.now = now_ms()
   |                    |   ctx.shard(k, f) ---- lock ------------>|  lazy expiry check
   |                    |                                          |  clone Bytes (refcount++)
   |                    |   <------------------ unlock ------------|  (if expired: remove + queue "DEL k")
   |                    |   encode "$n\r\n<v>\r\n" into out
   |                    |   no write happened -> no AOF barrier
   |<-------------------| write_all(out)
```

### SET k v EX 10, under `--appendfsync always`

```
 client        conn task                 shard k%64            AOF queue (mpsc)      AOF writer thread
   | SET k v EX 10 |                          |                       |                      |
   |-------------->| parse; execute:          |                       |                      |
   |               |  at = now + 10_000 (abs) |                       |                      |
   |               |  ctx.shard(k) -- lock -->|                       |                      |
   |               |                          | remove old entry,     |                      |
   |               |                          | insert new + TTL slot |                      |
   |               |                          | log "SET k v PXAT at"-->  send (never blocks) |
   |               |  <------- unlock --------|                       |                      |
   |               |  encode +OK into out     |                       |                      |
   |               |  wrote && policy==always:|                       |                      |
   |               |  sink.barrier() ---------------------------------> Barrier(tx)          |
   |               |  .await   (NO lock held) |                       |  drain queue, write(2) batch
   |               |                          |                       |  fdatasync()
   |               | <--------------------------------------------------- tx.send(())  ------|
   |<--------------| write_all(+OK)           |                       |                      |
```

* Pipelined batch = one `process()` call = at most **one** barrier and one `write_all`
  (the last record's barrier covers the earlier ones: the queue is FIFO).
* Under `everysec`/`no` there is no barrier; the reply goes out right after the
  shard lock is released.
* Reads of expired keys also queue a `DEL` (see 4, "expiry is journaled").

## 2. Concurrency model: (b) tokio multi-thread + 64 hash shards

**Chosen:** tokio multi-threaded runtime (default workers = CPU count), keyspace
split into `N = 64` shards by `hash(key) % N` (`RandomState`, so keys cannot be
crafted to collide), each shard behind its own `std::sync::Mutex`.

| | what it costs | what it buys |
|---|---|---|
| **(b) chosen** | a lock per shard; a hot key is a hot shard (all 50 clients `INCR`ing one key serialise on one mutex) | commands on different keys run in parallel across cores; critical sections are microseconds so blocking a worker briefly is cheaper than an async lock |
| (a) single-threaded runtime + one `tokio::sync::RwLock` | simplest and obviously deadlock-free | one core for everything; and an async lock held across work only buys cooperative fairness, not parallelism |
| (c) thread per connection + `std::sync::RwLock` | simplest of all; no async | a stack + a context switch per client (the ceiling is thousands of clients, not tens of thousands); one global lock still serialises writers |

**Why `Mutex`, not `RwLock`, per shard:** a read is not read-only. `GET` of an expired
key deletes it (lazy expiry) and queues a `DEL`, so reads need `&mut`. A `RwLock`
would need an upgrade path for exactly the case that matters, and with
sub-microsecond critical sections its extra bookkeeping costs more than it saves.

**Scaling ceiling, measured not claimed:** `results/bench_vs_redis*.csv`. With the
default single-threaded `redis-benchmark` client both servers sit near the same
client-bound level; with `--threads 4` redis-rs reaches ~133k ops/s in this
environment (see README for the numbers and the caveats). The ceiling for one key is one core
(the `INCR` test is the worst case by construction).

**Hard rule: no `std::sync` guard across an `.await`.** Enforced four ways:
1. shard locks exist only inside `Store::locked(key, FnOnce)`, a synchronous function
   (the guard cannot escape it);
2. every handler is a synchronous `fn(&mut Ctx, &[&[u8]]) -> Frame`;
3. `clippy::await_holding_lock` is `deny` in `Cargo.toml [lints.clippy]` (all targets,
   tests and benches included) and in `lib.rs`/`main.rs`; CI runs
   `cargo clippy --all-targets -- -D warnings`;
4. negative control: `scripts/mutation_check.sh` drops a deliberately bad file into a
   copy of the tree and requires clippy to fail with `await_holding_lock`
   (`results/mutation_report.txt`).

**Blocking inside the runtime:** a worker can block on a shard mutex for the
duration of someone else's critical section (microseconds; the longest are O(fields)
for `HGETALL`/`HDEL` on a huge hash). It never blocks on I/O: AOF records are queued
through an *unbounded* channel, so `send` cannot block, so lock-hold time never depends
on the disk. The price is that memory, not latency, absorbs a stalled disk
(`everysec`/`no`); under `always` each client waits for its own fsync, which bounds the
queue by clients x pipeline depth.

## 3. Lock inventory

| # | Lock | Protects | Held for | Taken while holding |
|---|---|---|---|---|
| 1 | `Store.shards[i]` (64 x `std::sync::Mutex<Shard>`) | that shard's `HashMap`, TTL index (`ttl_keys`), `wrote` flag | one command's single-key critical section: lookup + mutation + queueing the AOF record. Microseconds; O(fields) for `HGETALL`/`HDEL`/`HSET` with many fields | **#2 only**, via `Sender::send` |
| 2 | `std::sync::mpsc` queue (inside the channel) | AOF record queue | nanoseconds per `send`/`recv` | nothing (leaf) |
| 3 | `StopSignal` (`Mutex<bool>` + `Condvar`) | sweeper / `everysec` thread stop flag | nanoseconds | nothing (leaf) |

Not locks: `AofStats` and the `dirty` flag are atomics; the AOF file has exactly one
writer thread (plus the `everysec` thread, which only calls `fdatasync`, never `write`);
the shutdown flag is a `tokio::sync::watch`.

**Why no deadlock is possible**
1. *One shard lock per command.* Every command in scope names exactly one key, so a
   handler takes exactly one shard lock and never a second. `INCR` reads then writes
   **the same key inside one lock scope** (`Shard::incr`), which is also what makes it
   atomic. A lock graph with a single level has no cycles.
2. *Fixed order, one direction.* The only nesting is shard -> queue. The queue is a leaf
   and the AOF writer thread never takes a shard lock, so the reverse edge does not exist.
3. *Producers never wait on the consumer.* `record()` cannot block (unbounded), so a
   thread holding a shard lock is never waiting for the writer.
4. *The sweeper* locks one shard, for one round of <= 20 samples, then releases it before
   the next. It never holds two.
5. *No lock across `.await`* (rule above), so a parked task can never be holding a lock
   that another task on the same worker needs.

**Proof obligations, mechanised** (`tests/concurrency.rs`, all under a 30 s guard that
writes `results/deadlock_report.txt` on timeout):
50 tasks x 1000 `INCR` on one key = exactly 50 000 *and* the returned values are exactly
the set 1..=50000 (no lost update, no duplicate); 20 tasks on distinct keys see no
cross-talk; a 2.5 s mixed workload (SET PX / GET / DEL / EXPIRE / INCR / HSET / HDEL /
HGETALL / TTL / EXISTS over 40 shared keys on 8 shards, 32 connections, sweeper running)
neither hangs nor corrupts the TTL index; 16 raw OS threads hammering `Store` directly
lose no update. The mutation `incr-uses-two-lock-scopes` (read in one lock scope, write in
another) is caught by the 50 x 1000 test, i.e. the test really detects the bug class the
rule forbids.

## 4. Expiry

* **Deadlines** are absolute unix-ms (`u64`), compared as `now > deadline` (Redis's test).
* **Lazy**: every `Shard` method calls `live()` / `expire_if_needed()` before touching a key.
* **TTL index**: `Shard.ttl_keys: Vec<Box<[u8]>>` holds exactly the keys that have a TTL;
  each `Entry` remembers its slot, so insert/remove is O(1) (`swap_remove` + one fix-up).
  That is what makes uniform random sampling O(1). `check_invariants()` plus the
  60 000-step model test in `tests/expiry.rs` guard it.
* **Sweeper**: one thread, every 100 ms, per shard up to 16 rounds x 20 samples, continuing
  only while > 25 % of a round was expired (Redis' adaptive rule); one shard lock per round.
  Tradeoff: sampling is probabilistic, so an expired key can stay *physically present*
  for a few ticks (more with millions of TTL keys). It is never *observable*: any command
  touching it expires it first. No `DBSIZE`/`SCAN` exists to count physical keys.
* **Expiry is journaled.** Every expiry deletion (lazy or sweeper) queues `DEL key` to the
  AOF, as Redis does. Without it replay diverges: at runtime key `k` (deadline in the past)
  is dropped and `INCR k` creates `"1"`; replay, which expires nothing while loading,
  would `INCR` the *old* value and then drop it. Tests:
  `replay_matches_runtime_when_an_expired_key_is_reused_{lazy,sweeper}_path`; mutation
  `lazy-expiry-not-journaled` is caught.

## 5. Persistence model

* **What is in the AOF:** RESP arrays of bulk strings, one per *effect* (format checked
  against a real Redis AOF: `results/real_redis_aof_sample.txt`). `SET k v EX 10` -> `SET k v
  PXAT <abs ms>`; `EXPIRE` -> `PEXPIREAT <abs ms>` (or `DEL` if already due); expiry
  deletions -> `DEL`; `INCR`, `HSET`, `HDEL` as is; failures and reads are not logged.
  There is no `SELECT 0` header (single keyspace). No rewrite/compaction: the file only grows.
* **Order:** the record is queued while the shard lock is held, so two writes to the
  same key appear in the log in the order they were applied.
* **When fsync happens:** `always` = writer thread `write(2)`s each drained batch then
  `fdatasync`s once (group commit), then releases every client waiting in that batch;
  `everysec` = writer `write(2)`s immediately, a *separate* thread `fdatasync`s once per
  second if anything was written; `no` = never, except one final `fsync` at graceful
  shutdown. Observable in tests via `AofStats.fsyncs`
  (`fsync_policy_is_observable_*`).
* **Crash window:**

  | policy | process killed (`kill -9`) | OS crash / power loss |
  |---|---|---|
  | `always` | nothing acknowledged is lost | nothing acknowledged is lost |
  | `everysec` | only records still queued in-process (micro- to milliseconds) | up to ~1 s of acknowledged writes |
  | `no` | same as `everysec` | whatever the kernel had not flushed (typically up to ~30 s) |

  The tests can kill the process, not the machine; the right-hand column is argued from
  where `fdatasync` sits, and checked structurally by the counters.
* **Replay (before the listener is bound):** parse the file with the same `parse_request`,
  execute each record through the same `execute` with `replay = true` (accepts `PXAT` /
  `PEXPIREAT`, rejects relative `EX`/`PX`) and `now = 0`, i.e. *nothing is expired while
  loading* (Redis does the same), so replay is a pure function of the log. Absolute deadlines
  mean a key whose time passed during downtime is gone at first access. Tests:
  `px_key_does_not_resurrect_after_downtime`.
* **Torn tail -> truncate and start:** if the file ends in the middle of a command, the
  tail is cut (`ftruncate` + `fsync`), logged to stderr, and described in
  `<aof>.corruption-report.txt` (`bytes_dropped`, `last_good_offset`, `offending_hex`).
  Tested at **every byte offset** of the last record (`a_tear_at_every_byte_offset_...`).
* **Anything else -> refuse to start** (exit 1, offset in the message, file untouched):
  garbage mid-file, a valid record that fails to re-execute. We do not guess. In particular
  a zero-filled tail (some filesystems after a crash) is *not* a torn command and is
  refused; an operator must truncate it.
* **I/O errors are fatal** (exit 1): acknowledging a write that cannot be persisted is worse
  than stopping. (Redis behaves the same under `always`.)
* **Graceful shutdown** order is in `server.rs`: stop accepting -> tell connections ->
  drain (<= `--drain-timeout-ms`, then abort) -> stop sweeper -> drain + fsync the AOF ->
  exit 0. A second signal exits 1 immediately.

## 6. How to add a new command (exactly 5 steps)

Verified by actually adding `TYPE` and reverting (`results/add_command_verification.txt`).
Steps 1-4 took four edits and the oracle confirmed the reply byte for byte.

1. **`src/store.rs`** - add a `Shard` method. It is the *only* code that touches the maps.
   Take `now`, call `self.live(key, now)` first, and if it mutates anything finish with
   `self.log(&[b"CMD", ...])` so the effect is journaled.
   `pub fn type_name(&mut self, key: &[u8], now: u64) -> Option<&'static str>`
2. **`src/command.rs`** - write the handler `fn cmd_type(ctx: &mut Ctx<'_>, args: &[&[u8]]) -> Frame`.
   Reach data only through `ctx.shard(key, |sh, now| sh.type_name(key, now))`. Return
   `store_error(e)` for `StoreError`, `arity_error(..)` etc. - do not invent strings.
3. **`src/command.rs`** - add the row to the `COMMANDS` table:
   `CommandSpec { name: "type", arity: 2, internal: false, handler: cmd_type }`.
   (`arity`: `n > 0` exactly n args including the name, `n < 0` at least `-n`.
   `internal: true` = replay only.) The arity error is produced for you.
4. **`tests/protocol_oracle.rs`** - add rows to `script()`:
   `c(&["TYPE"]), c(&["TYPE", "never"]), c(&["TYPE", "k"])`, ... then run
   `make oracle-diff`. The real Redis, not your assumption, defines the expected bytes.
   (A write command also needs an expected record in
   `aof_records_the_effect_of_each_write_in_redis_format`, `tests/persistence_crash.rs`,
   and a replay line if it needs a new internal form.)
5. **`SPEC.md` + `README.md`** - add the command to the command tables; remove it from the
   out-of-scope list if it was there.

## 7. Decisions you should be able to defend

| Decision | Why | Alternative rejected |
|---|---|---|
| Requests parsed as `&[u8]` slices of the read buffer | zero copies, zero refcounting on the hot path (~52 ns per SET parse, `results/parser_bench.txt`) | `Bytes` slices: refcount traffic, and storing one would pin the whole read buffer |
| Values copied into the store (`Bytes::copy_from_slice`) | a stored slice of a 64 KiB read buffer would keep the whole buffer alive per key | zero-copy store |
| `HGETALL` in insertion order via a per-field sequence number | matches Redis for small hashes (diffed by the oracle), O(1) everything else | `IndexMap`-style vector (O(n) delete) or random `HashMap` order (differs from Redis) |
| Reads that expire a key journal a `DEL` | replay = runtime (section 4) | no journal for lazy expiry: divergent replay |
| Unbounded AOF queue | a shard lock is never held across disk I/O | bounded channel: backpressure but a lock held while waiting for the disk |
| Inline commands rejected, connection closed | out of scope; cannot resynchronise after an inline line | best-effort inline support |
| TTL rounds like Redis | the oracle says so (SPEC 4.1) | truncation (the initial spec's wording) |
| `process::exit(1)` on AOF write/fsync error | never acknowledge what is not durable | log and continue |
