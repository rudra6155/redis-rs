# redis-rs

A Redis subset written in Rust from scratch — RESP2 on the wire, a sharded
in-memory store with TTLs, append-only persistence, and an explicit concurrency
model. Every reply is byte-identical to `redis:7` for the supported command set,
verified by diffing raw bytes against a real `redis-server`.

## Quick start

```sh
make install          # cargo build --release
make test             # all tests incl. the oracle diff (needs Docker, starts redis:7)
make test-nodocker    # everything except the oracle tests (CI without docker-in-docker)
make bench-vs-redis   # redis:7 vs redis-rs with the official redis-benchmark -> results/
make docker           # minimal image;  make docker-test  runs the suite inside containers
target/release/redis-rs --port 6379 --appendonly yes --appendfsync everysec
```

`make install` takes ~14 s, `make test` (69 tests, oracle included) ~23 s. The
Docker image is 38.8 MB (distroless, non-root) and answers PING/SET/GET to the
official `redis-cli`.

**Flags:** `--bind` `--port` (0 = any; prints `Ready to accept connections on ...`)
`--appendonly yes|no` `--appendfsync` `--appendfilename` `--dir` `--sweeper yes|no`
`--shards` `--threads` `--maxclients` `--drain-timeout-ms` `--version`.

## What this is / what it's NOT

**Is:** `PING` `ECHO` `SET` (`EX`/`PX`) `GET` `DEL` `EXISTS` `TTL` `EXPIRE` `INCR`
and hash commands `HSET` `HGET` `HDEL` `HLEN` `HGETALL`; RESP2 only (all seven types
parse, encode, and round-trip); AOF persistence with `--appendfsync always|everysec|no`;
tokio multi-threaded runtime over 64 hash-sharded locks; graceful shutdown on
SIGINT/SIGTERM. Replies match `redis:7.4.12` byte for byte across 199 test rows
(186 commands + 13 wire cases), all PASS.

**Is NOT:** RESP3, pub/sub, `MULTI`/`EXEC`, `SCAN`, replication, cluster,
ACL/`AUTH`, `SELECT`, scripting, `WAIT`, `BITFIELD`, streams, RDB,
`BGREWRITEAOF`, TLS, `CONFIG`/`INFO`/`CLIENT`/`DBSIZE`/`KEYS`. All of these
answer `ERR unknown command ...` like any unknown command. "Redis-compatible"
here means exactly *byte-identical replies for the in-scope set*; it is not a
drop-in replacement.

**Documented divergences** (each asserted by a test): inline commands rejected
(D1), requests limited to 1 048 576 arguments (D2), bulk string must be followed
by `\r\n` (D3), `SET` accepts only `EX`/`PX` (D4), `DEL`/`EXISTS` take exactly
one key (D5), `EXPIRE` has no `NX/XX/GT/LT` (D6). See [SPEC.md](SPEC.md) for
the full list and rationale. Tested platform: Linux (container on Docker
Desktop); macOS/Windows-native builds were not tested.

## Supported commands

| Command | Request | Reply | Error cases |
|---|---|---|---|
| `PING` | `PING [msg]` | `+PONG` or bulk `msg` | arity (`PING a b`) |
| `ECHO` | `ECHO msg` | bulk | arity |
| `SET` | `SET k v [EX s \| PX ms]` | `+OK`; TTL set atomically; no EX/PX clears the TTL | arity; `ERR syntax error`; `ERR value is not an integer or out of range`; `ERR invalid expire time in 'set' command` |
| `GET` | `GET k` | bulk or `$-1` | arity; `WRONGTYPE` on a hash |
| `DEL` | `DEL k` | `:1` removed / `:0` (single key) | arity |
| `EXISTS` | `EXISTS k` | `:1` / `:0` (single key) | arity |
| `TTL` | `TTL k` | `:-2` missing, `:-1` no TTL, else seconds rounded to nearest (as Redis) | arity |
| `EXPIRE` | `EXPIRE k s` | `:1` set (replaces a TTL), `:0` missing; `s <= 0` deletes | arity; not-an-integer; `invalid expire time in 'expire' command` |
| `INCR` | `INCR k` | new value; creates `1`; keeps the TTL | arity; `WRONGTYPE`; `ERR value is not an integer or out of range`; `ERR increment or decrement would overflow` |
| `HSET` | `HSET k f v [f v ...]` | number of new fields | arity (odd tail); `WRONGTYPE` on a string |
| `HGET` | `HGET k f` | bulk or `$-1` | arity; `WRONGTYPE` |
| `HDEL` | `HDEL k f [f ...]` | fields removed (last field removed deletes the key) | arity; `WRONGTYPE` |
| `HLEN` | `HLEN k` | integer | arity; `WRONGTYPE` |
| `HGETALL` | `HGETALL k` | flat `[f, v, f, v ...]` in insertion order | arity; `WRONGTYPE` |

Unknown command: `ERR unknown command 'FOO', with args beginning with: 'a' 'b' `
(trailing space, 128-byte argument budget, exactly as Redis). All error strings
are diffed byte for byte against real Redis.

## Expiry

Lazy (every command checks the key it touches) + a background sweeper that wakes
every 100 ms and samples a bounded number of TTL keys per shard. A key set with
`PX 50` is unreadable *and physically deleted* by 150 ms even without any
command touching it. Sampling means a key can briefly stay physically present
after its deadline; nothing user-visible can observe that (no `DBSIZE`/`SCAN`).

**TTL rounding:** Real Redis 7.4.12 returns 100 for `SET k v EX 100` followed by
`TTL k` (it computes `(ms + 500) / 1000`). I matched the real server rather than
truncating — it's a one-line change in `Shard::ttl` if you want truncation.

## Concurrency

tokio multi-threaded runtime, 64 shards by hash, one `std::sync::Mutex` per
shard. Every command in scope touches exactly one key → one lock, never two.
`INCR` reads and writes under a single lock scope. No guard is ever held across
an `.await` — `clippy::await_holding_lock` is `deny` for all targets, and a
deliberately bad file is rejected as a negative control.

Tests: 50 tasks × 1000 `INCR` = exactly 50 000 with replies `1..=50000`; 20
tasks on distinct keys see no cross-talk; a 2.5 s mixed workload with the
sweeper running neither hangs nor corrupts the TTL index. No deadlock was ever
observed. See [ARCHITECTURE.md](ARCHITECTURE.md) for the full defense.

## Persistence

AOF format matches real Redis — RESP arrays of bulk strings, one per *effect*
(`SET k v EX 10` → `SET k v PXAT <absolute ms>`, `EXPIRE` → `PEXPIREAT`,
expiry deletions → `DEL`). Replay runs before the port is bound, expires nothing
while loading, and uses absolute deadlines.

| `--appendfsync` | when data hits disk | lost on `kill -9` | lost on OS crash |
|---|---|---|---|
| `always` | group-committed `fdatasync` before the reply | nothing | nothing |
| `everysec` | separate thread, once a second | micro- to milliseconds | up to ~1 s |
| `no` | never (final `fsync` at shutdown) | same as `everysec` | whatever the kernel hadn't flushed |

Process death tested with real `kill -9` + restart under all three policies.
Torn final command → truncate, log, report in `<aof>.corruption-report.txt`;
any other corruption → refuse to start, file untouched.

## Benchmarks

Using the official `redis-benchmark` from the `redis:7` image — not a client of
mine. Both servers in containers, benchmark joined to the server's network
namespace. 5 runs per server, alternating, medians reported.

**Environment** (`results/hardware.json`): Ryzen 7 7435HS, 16 cores, Docker
Desktop / WSL2, rustc 1.99.1, redis 7.4.12.

**Headline: `-n 100000 -c 50 -t set,get`** (single client thread)

| op | redis ops/s | redis-rs ops/s | ratio | redis p50 / p99 (ms) | redis-rs p50 / p99 (ms) |
|---|---:|---:|---:|---:|---:|
| SET | 66,578 | 59,312 | 0.89× | 0.375 / 1.375 | 0.407 / 1.071 |
| GET | 68,399 | 60,938 | 0.89× | 0.367 / 1.359 | 0.407 / 0.975 |

**With 4 client threads** (`--threads 4`)

| op | redis ops/s | redis-rs ops/s | ratio | redis p50 / p99 (ms) | redis-rs p50 / p99 (ms) |
|---|---:|---:|---:|---:|---:|
| SET | 49,925 | 132,979 | 2.66× | 0.847 / 1.791 | 0.159 / 0.615 |
| GET | 57,045 | 133,156 | 2.33× | 0.823 / 1.711 | 0.159 / 0.551 |

With a single client thread redis-rs sits at ~0.89× of Redis in throughput.
With 4 threads the order flips — redis-rs reaches ~133k ops/s vs Redis's ~50k.
My hypothesis is Redis's single event-loop thread saturates while redis-rs
spreads I/O over the multi-threaded runtime, but I didn't profile it. Absolute
numbers are from a Docker VM on a laptop with 17 other containers running, so
treat the ratios as indicative. Full data and random-key results in `results/`.

**Parser in isolation** (criterion, no I/O): parsing a `SET` takes ~52 ns, a
`GET` ~32 ns, a 16-command pipeline ~668 ns.

## Testing strategy

| Evidence | What it shows |
|---|---|
| `results/oracle_report.txt` | 199 rows sent to both redis-rs and redis 7.4.12, raw bytes compared: 199 PASS |
| `results/mutation_report.txt` | 14 injected bugs + lint negative control: all 15 caught by named tests |
| `results/stability_check.txt` | full suite, 10 plain + 5 under CPU load: 0 failed runs |
| `results/fresh_clone_run.txt` | clean-tree `make install` / `make test` timing |
| `results/aof_corruption_report.txt` | torn-tail truncation report from the crash tests |

## How to add a new command

Five steps, each naming a file and function:
[ARCHITECTURE.md § 6](ARCHITECTURE.md#6-how-to-add-a-new-command-exactly-5-steps).
Verified by following them to add `TYPE` (four edits, checked against real Redis)
and reverting.

## Repo map

`src/` — see [ARCHITECTURE.md](ARCHITECTURE.md) | `tests/` — unit, oracle,
concurrency, expiry, crash recovery, error semantics + `tests/common/` |
`benches/ops.rs` | `scripts/` | `results/` (committed evidence) | `Makefile`,
`Dockerfile`, `.github/workflows/ci.yml`.

## License

MIT
