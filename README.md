# redis-rs

A Redis subset in Rust, built to be taken apart and rebuilt on a whiteboard: RESP2 on
the wire, a sharded in-memory store with TTLs, an append-only file, and an explicit
concurrency model. Behaviour is checked against a **real `redis-server`** rather than
against my own reading of the docs.

Every factual claim below carries a tag like **[C3]**. [CLAIMS.md](CLAIMS.md) maps each tag to the
committed artifact that backs it, the command that regenerates it, and its status. A claim without a
VERIFIED row would be marked *unverified* right here; there are none at the moment except the one
GitHub-hosted CI run, which is marked PENDING **[C22]**.

## What this is / what this is NOT

**Is:** `PING` `ECHO` `SET` (`EX`/`PX`) `GET` `DEL` `EXISTS` `TTL` `EXPIRE` `INCR` and the hash
commands `HSET` `HGET` `HDEL` `HLEN` `HGETALL`; RESP2 only (simple string, error, integer, bulk, array,
null bulk and null array all parse, encode and round-trip **[C1]**); AOF persistence with
`--appendfsync always|everysec|no`; tokio multi-threaded runtime over 64 hash-sharded locks;
graceful shutdown on SIGINT/SIGTERM. Replies are byte-identical to `redis:7` (measured: 7.4.12) for
this command set, the four required error cases, and a set of wire-level cases (199 rows,
all PASS) **[C3][C4]**.

**Is NOT** (see [SPEC.md](SPEC.md) for the full list and the reasons):
RESP3, pub/sub, `MULTI`/`EXEC`, `SCAN`, replication, cluster, ACL/`AUTH`, `SELECT`, scripting,
`WAIT`, `BITFIELD`, streams, RDB, `BGREWRITEAOF` (the AOF only grows), TLS, `CONFIG`/`INFO`/`CLIENT`/
`DBSIZE`/`KEYS`. All of those answer `ERR unknown command ...` like any unknown command **[C27]**.
"Redis-compatible" here means exactly *byte-identical replies for the in-scope set*; it is not a
drop-in replacement. Documented divergences (all asserted by a test **[C27]**): inline commands are
rejected (D1), requests are limited to 1 048 576 arguments (D2), a bulk string must be followed by
`\r\n` (D3), `SET` accepts only `EX`/`PX` (D4), `DEL`/`EXISTS` take exactly one key (D5),
`EXPIRE` has no `NX/XX/GT/LT` (D6). Tested platform: Linux (a container on Docker Desktop);
macOS/Windows-native builds were not run **[C23]**.

## Quick start

```sh
make install          # cargo build --release
make test             # all tests incl. the oracle diff (needs Docker, starts redis:7)
make test-nodocker    # everything except the oracle tests (CI without docker-in-docker)
make bench-vs-redis   # redis:7 vs redis-rs with the official redis-benchmark -> results/
make docker           # minimal image;  make docker-test  runs the suite inside containers
target/release/redis-rs --port 6379 --appendonly yes --appendfsync everysec
```

On a clean copy of the tree (cold build directory, crates.io cache warm) `make install` took 14 s
and `make test` (69 tests, oracle included) took 23 s, against a 180 s budget **[C17]**.
`make test-nodocker` runs 67 of those 69 tests (all but the two oracle tests) **[C24]**.
`make docker-test` passes and removes its containers and network **[C17]**; the image is
38.8 MB (distroless, non-root) and answers PING/SET/GET to the official `redis-cli` **[C18]**.

Flags: `--bind` `--port` (0 = any; the chosen address is printed as `Ready to accept connections on ...`)
`--appendonly yes|no` `--appendfsync` `--appendfilename` `--dir` `--sweeper yes|no` `--shards`
`--threads` `--maxclients` `--drain-timeout-ms` `--version`.

## Supported commands

| Command | Request | Reply | Error cases |
|---|---|---|---|
| `PING` | `PING [msg]` | `+PONG` or bulk `msg` | arity (`PING a b`) |
| `ECHO` | `ECHO msg` | bulk | arity |
| `SET` | `SET k v [EX s \| PX ms]` | `+OK`; TTL set atomically; no EX/PX **clears** the TTL | arity; `ERR syntax error`; `ERR value is not an integer or out of range`; `ERR invalid expire time in 'set' command` |
| `GET` | `GET k` | bulk or `$-1` | arity; `WRONGTYPE` on a hash |
| `DEL` | `DEL k` | `:1` removed / `:0` (single key) | arity |
| `EXISTS` | `EXISTS k` | `:1` / `:0` (single key) | arity |
| `TTL` | `TTL k` | `:-2` missing, `:-1` no TTL, else seconds **rounded to nearest** (as Redis, see below) | arity |
| `EXPIRE` | `EXPIRE k s` | `:1` set (replaces a TTL), `:0` missing; `s <= 0` deletes | arity; not-an-integer; `invalid expire time in 'expire' command` |
| `INCR` | `INCR k` | new value; creates `1`; keeps the TTL | arity; `WRONGTYPE`; `ERR value is not an integer or out of range`; `ERR increment or decrement would overflow` |
| `HSET` | `HSET k f v [f v ...]` | number of **new** fields | arity (odd tail); `WRONGTYPE` on a string |
| `HGET` | `HGET k f` | bulk or `$-1` | arity; `WRONGTYPE` |
| `HDEL` | `HDEL k f [f ...]` | fields removed (last field removed deletes the key) | arity; `WRONGTYPE` |
| `HLEN` | `HLEN k` | integer | arity; `WRONGTYPE` |
| `HGETALL` | `HGETALL k` | flat `[f, v, f, v ...]` in insertion order | arity; `WRONGTYPE` |

Unknown command: `ERR unknown command 'FOO', with args beginning with: 'a' 'b' ` (trailing space, 128-byte
argument budget, exactly as Redis). Arity: `ERR wrong number of arguments for 'get' command`. Wrong
type: `WRONGTYPE Operation against a key holding the wrong kind of value`. All of these are
diffed byte for byte against the real Redis **[C3][C4]**.

**Expiry** is lazy (every command checks the key it touches) plus a background sweeper that wakes every 100 ms and samples a
bounded number of TTL keys per shard. A key set with `PX 50` is unreadable *and physically deleted* by 150 ms even though no command
touched it; with the sweeper disabled lazy expiry alone keeps every read and write path correct **[C6]**. Sampling means a
key can briefly stay physically present after its deadline; nothing user-visible can observe that (no `DBSIZE`/`SCAN`) **[C6]**.

**TTL deviation from the original brief.** The brief says TTL is "truncated toward zero". Real Redis
7.4.12 returns **100** for `SET k v EX 100` followed by `TTL k` (it computes `(ms + 500) / 1000`), so a
truncating server would visibly disagree with it. I followed the oracle; a committed oracle row
(`SET round v PX 1900` then `TTL` must be exactly `:2`) and the expiry tests pin it **[C5]**. It is a
one-line change in `Shard::ttl` if truncation is wanted.

## Concurrency (summary; defence in [ARCHITECTURE.md](ARCHITECTURE.md))

Model **(b)**: tokio multi-threaded runtime, 64 shards by hash, one `std::sync::Mutex` per shard.
Every in-scope command touches exactly one key, so a handler holds **one** lock, never two; `INCR` reads
and writes under a single lock scope. No guard is ever held across an `.await`: shard locks exist only
inside the synchronous `Store::locked`, handlers are synchronous, and `clippy::await_holding_lock` is
`deny` for every target (a deliberately bad file is rejected by clippy: negative control) **[C8]**.
Tests: 50 tasks x 1000 `INCR` give exactly 50 000 and the replies are exactly the set `1..=50000`; 20
tasks on distinct keys see no cross-talk; a 2.5 s mixed workload with the sweeper running neither hangs
nor corrupts the TTL index **[C7]**. No deadlock was ever observed; `results/deadlock_report.txt`
does not exist because the 30 s guard never fired.

## Persistence (summary)

The AOF holds RESP arrays of bulk strings, one per *effect*: `SET k v EX 10` is stored as
`SET k v PXAT <absolute ms>`, `EXPIRE` as `PEXPIREAT`, and expiry deletions as `DEL`, which is what a real Redis
AOF contains **[C9]**. Replay runs before the port is bound, expires nothing while loading, and uses absolute
deadlines, so a `PX 200` key is still gone after 1.6 s of downtime **[C12]**.

| `--appendfsync` | when data hits disk | lost on `kill -9` | lost on OS crash / power loss |
|---|---|---|---|
| `always` | group-committed `fdatasync` before the reply | nothing acknowledged | nothing acknowledged |
| `everysec` | separate thread, once a second | only the in-process queue (micro- to milliseconds) | up to ~1 s |
| `no` | never (final `fsync` at shutdown) | same as `everysec` | whatever the kernel had not flushed |

Process death was tested with real `kill -9` + restart under all three policies (1000 keys each)
**[C10]**; the fsync policy itself is observed through counters (`always` has synced before the client is
released, `everysec` does not fsync inline but does within 1.4 s, `no` only at shutdown) **[C11]**. **The OS-crash column is
argued from where `fdatasync` sits; I cannot pull the power cord in a test.**
A torn final command is truncated, logged, and reported in `<aof>.corruption-report.txt`
(tested at every byte offset of the last record); any other corruption refuses to start and leaves the
file untouched **[C13]**. SIGINT/SIGTERM: stop accepting, finish in-flight commands, drain and fsync the AOF,
exit 0; a second signal forces exit 1 **[C14]**.

## How the claims are verified

| Evidence | What it shows |
|---|---|
| `results/oracle_report.txt` | 199 rows (186 commands + 13 wire cases) sent to redis-rs and redis 7.4.12, raw bytes compared: 199 PASS **[C3]** |
| `results/mutation_report.txt` | 14 deliberately injected bugs + the lint negative control: all 15 are caught by the tests named in the report **[C16]** |
| `results/stability_check.txt` | the whole suite, 10 plain + 5 under CPU load: 0 failed runs **[C15]** |
| `results/ci_local_run.txt` | the commands of `.github/workflows/ci.yml` run locally: all pass. A GitHub-hosted run is **PENDING** (repo not pushed yet) **[C22]** |
| `results/fresh_clone_run.txt`, `docker_acceptance.txt` | clean-tree `make install`/`make test`, `docker-test`, image smoke **[C17][C18]** |
| `results/real_redis_aof_sample.txt` | what a real Redis writes to its AOF for these commands **[C9]** |
| `results/aof_corruption_report.txt` | the report written by the torn-tail test **[C13]** |
| `results/add_command_verification.txt` | the "add a command" recipe followed for real (`TYPE`) and reverted **[C21]** |

## Benchmarks

Tool: the **official `redis-benchmark`**, taken from the `redis:7` image, not a client of mine. That removes
the objection that *my* client is the bottleneck. It does not remove the possibility that the official
client is one (it is single-threaded by default), which is why a `--threads 4` suite is included below **[C25]**.
Method: both servers in containers, each run a throw-away container joined to the *network namespace of the
server under test* (`127.0.0.1`, same machine); per brief `-n 100000 -c 50 -t set,get -q` plus a second `--csv`
run per iteration for p50/p99 (**`redis-benchmark` has no `--latency` flag**; that belongs to `redis-cli`; `--csv`
is its percentile output); 5 runs per server, alternating, after a discarded warm-up; medians reported. The
reference runs `--save "" --appendonly no`, redis-rs runs with no AOF **[C25]**.

Environment (`results/hardware.json`): AMD Ryzen 7 7435HS, 16 cores, 11.5 GB visible to the Docker
VM, Docker Desktop on Windows 11, kernel 6.18.33.2-microsoft-standard-WSL2, rustc 1.99.1, redis 7.4.12
(`redis:7`, digest recorded in the file), run 2026-10-29T16:16:25Z **[C19]**.

**Headline: `-n 100000 -c 50 -t set,get` (one key, 1 client thread)** (`results/bench_vs_redis.csv`, `results/bench_vs_redis.png`) **[C19]**

| op | redis ops/s | redis-rs ops/s | redis-rs / redis | redis p50 / p99 (ms) | redis-rs p50 / p99 (ms) |
|---|---:|---:|---:|---:|---:|
| SET | 66,578 | 59,312 | 0.89x | 0.375 / 1.375 | 0.407 / 1.071 |
| GET | 68,399 | 60,938 | 0.89x | 0.367 / 1.359 | 0.407 / 0.975 |

**Supplementary: `-r 100000` (random keys)** (`results/bench_vs_redis_random_keys.csv`) **[C19]**

| op | redis ops/s | redis-rs ops/s | redis-rs / redis | redis p50 / p99 (ms) | redis-rs p50 / p99 (ms) |
|---|---:|---:|---:|---:|---:|
| SET | 69,638 | 56,211 | 0.81x | 0.359 / 1.183 | 0.431 / 1.095 |
| GET | 68,918 | 62,539 | 0.91x | 0.359 / 1.247 | 0.391 / 0.919 |

**Supplementary: `--threads 4` (4 client threads)** (`results/bench_vs_redis_threads4.csv`) **[C19]**

| op | redis ops/s | redis-rs ops/s | redis-rs / redis | redis p50 / p99 (ms) | redis-rs p50 / p99 (ms) |
|---|---:|---:|---:|---:|---:|
| SET | 49,925 | 132,979 | 2.66x | 0.847 / 1.791 | 0.159 / 0.615 |
| GET | 57,045 | 133,156 | 2.33x | 0.823 / 1.711 | 0.159 / 0.551 |

(Generated by `python3 scripts/bench_table.py` from the CSVs; per-run percentiles are in `results/p99_latency.csv`.)

**How to read this, and what not to conclude**

* With the default single-client-thread tool, redis-rs is at 0.81x to 0.91x of real Redis in throughput, with a slightly
  higher p50 (0.41 vs 0.37 ms) and a lower p99 (about 1.0 vs 1.4 ms) **[C19]**. I did not investigate the p99 difference.
* With 4 client threads the order flips: redis-rs reaches about 133k ops/s and real Redis 50k to 57k **[C19]**. Real Redis
  getting *slower* than in the 1-thread run is unexplained; I did not profile it. *Hypothesis, not verified:* Redis' single
  event-loop thread is what saturates (its default has no I/O threads) while redis-rs spreads socket I/O over a multi-threaded
  runtime, and syscalls are expensive inside this VM.
* The default benchmark hits **one key**, i.e. one shard of 64, so it cannot show sharding at all; the `-r` suite
  does not show it either at this client rate. Whether redis-rs would beat Redis on bare metal is not established here.
* Absolute numbers are bounded by a Docker Desktop VM on a laptop, and the machine was **not quiet**: a local Supabase stack and
  a Lago stack (17 other containers, each under 5 % CPU in `results/bench_environment.txt`, sampled minutes after the run
  when they had been up for hours) were running, one of them restart-looping.
  Treat the ratios as indicative, not as a ranking of the two designs.
* Redis is single-threaded and redis-rs is multi-threaded by design, so this is not a like-for-like core count.
* Not claimed anywhere: that redis-rs is "faster than Redis".

**Parser in isolation** (criterion, no I/O; `results/parser_bench.txt`) **[C20]**: parsing a 3-argument `SET` request
takes about 52 ns (~19 M requests/s per core), a `GET` about 32 ns, a 16-command pipeline about 668 ns; a store `GET` hit about
58 ns and a full `execute` of `GET`/`SET` (no parse, no I/O) about 95/163 ns. At 0.4 ms p50 per request the parser is on the order of
0.01 % of what the network benchmark measures, i.e. the headline numbers are about syscalls and scheduling, not parsing.

## Things that broke

Honest accounting, split by how I found it. "Observed" = it failed or was wrong on a real run. "Near-miss" = I
caught it in review before it could ship, and the **mutation check proves the named test fails without the fix**
(`results/mutation_report.txt`) **[C16]**. Not every category in the brief produced a real failure, and I would
rather say so than invent one: no deadlock was ever observed, and the AOF torn-write handling never failed.

**Observed**

1. *TTL semantics (the brief was wrong).* Symptom: `SET k v EX 100; TTL k` is 100 on real Redis; a server that truncates, as the
   brief specifies, says 99. Cause: the brief's rule did not match Redis (`(ms + 500) / 1000`), found by probing the oracle before
   writing code. Fix: round. File: `src/store.rs` `Shard::ttl`. Pinned by an exact oracle row and `tests/expiry.rs`; mutation
   `ttl-truncates-instead-of-rounds` is caught **[C5]**. A first draft of the oracle normalisation (TTL within +-1 s) would have
   accepted the wrong rule, so a separate exact-value row was added.
2. *RESP parser edge case: the bulk terminator.* Symptom: I had planned to mirror Redis; the oracle showed Redis does **not** check the
   two bytes after a bulk payload (`*1\r\n$4\r\nPINGxx` answers `+PONG`). Cause: Redis skips them blindly. Decision: be strict and
   reject with a protocol error (divergence D3). File: `src/resp.rs` `parse_request`; test `bulk_terminator_must_be_crlf`; mutation
   `bulk-terminator-not-checked` is caught **[C2][C16]**.
3. *The oracle harness itself.* The first run of the "start redis:7 with `docker run`" path, exercised from a container, failed
   three ways: it left the `redis:7` container running when start-up failed (the guard was created after the readiness wait), it only
   tried the published loopback port (unreachable from a container on the Docker host), and my own edit had turned the `\r\n` in the
   PING probe into raw newlines, so Redis never answered. Fix: RAII `Container` guard created immediately after `docker run`, candidate
   addresses (published port, then bridge IP), per-candidate error reporting. File: `tests/common/mod.rs`. Re-run: passes, and the
   running-container count is the same before and after **[C26]**.
4. *Evidence files that did not show what they claimed.* The first AOF sample lost every CR on the Windows pipe, so it proved nothing
   about CRLF framing (fixed: render with `cat -A` inside the container, `scripts/real_aof_sample.sh`); the first benchmark chart had
   its legend over a bar label and a footer cut off at the edge (fixed after rendering and looking at it, `scripts/plot_bench.py`)
   **[C9][C19]**.
5. *Clippy 1.99 on the first lint run:* `manual_range_contains`, `manual_is_multiple_of` (x2), `chunks_exact_to_as_chunks`, `type_complexity`
   on `parse_request`'s return type, `write_literal` in a test. All fixed at the cause, no `allow`. Files: `src/command.rs`, `src/store.rs`,
   `src/resp.rs`, `tests/protocol_oracle.rs`.

**Near-misses, each proven catchable by mutation**

6. *Replay diverging from runtime (the TTL/replay bug).* Hazard: key `k` expires; at runtime `INCR k` starts a fresh `"1"`; replay expires
   nothing while loading, so it would `INCR` the *old* value and then drop it, yielding a different final state. Fix: every expiry
   deletion, lazy or by the sweeper, journals a `DEL`. File: `src/store.rs` `expire_if_needed` / `sweep`. Tests:
   `replay_matches_runtime_when_an_expired_key_is_reused_{lazy,sweeper}_path`; mutation `lazy-expiry-not-journaled` is caught **[C12][C16]**.
7. *Signal-handler race.* Hazard: a SIGINT arriving right after the "Ready" line would hit the default action and kill the process with a
   non-zero status. Fix: handlers are installed before "Ready" is printed. Files: `src/signal.rs` `ShutdownSignals::install`, `src/main.rs`.
   Test: `sigint_immediately_after_ready_still_exits_zero` (15 rounds); mutation `signals-installed-after-ready-line` is caught **[C14][C16]**.
8. *Lost update from two lock scopes (the lock-ordering rule).* Hazard: implementing `INCR` as a read in one lock scope and a write in another.
   Fix: one scope in `Shard::incr`. Test: the 50 x 1000 `INCR` test (and the replies must be exactly `1..=50000`); mutation `incr-uses-two-lock-scopes`
   is caught **[C7][C16]**.

## How to add a new command

Five steps, each naming a file and function: [ARCHITECTURE.md section 6](ARCHITECTURE.md#6-how-to-add-a-new-command-exactly-5-steps).
Verified by following it cold to add `TYPE` (four edits, checked against the real Redis) and reverting byte-identically; step 5 is a
documentation edit and was not exercised mechanically **[C21]**.

## Provenance and borrowings

No code was copied, and I did not open the source of `mini-redis` or of any Redis client while writing this (self-attested; there is no
artifact that can prove a negative) **[C28]**. What is borrowed are Redis's own ideas, in two groups:

* *Observable on the wire, so checked against the real server:* the exact error wording, the `string2ll` integer rules, `TTL` rounding,
  and the AOF forms `SET ... PXAT`, `PEXPIREAT` and `DEL` for expiry **[C3][C9]**.
* *Descriptions of Redis internals, from my memory of its source and **not verified here*** (they are design inspiration, not claims
  the tests depend on): the active-expire algorithm (sample, repeat while more than 25 % expired), "nothing expires while an AOF is being
  loaded", and the group-commit shape of `appendfsync always` **[C29]**.

The tokio task-per-connection structure is the common idiom.

## Repository map

`src/` as in [ARCHITECTURE.md](ARCHITECTURE.md) (plus `lib.rs`, so tests and benches can link the real code) |
`tests/{resp_unit,protocol_oracle,concurrency,expiry,persistence_crash,error_semantics}.rs` + `tests/common/` |
`benches/ops.rs` | `scripts/` (`bench_vs_redis.sh`, `oracle_diff.sh` as in the brief; plus `smoke.sh`, `docker_smoke.sh`, `mutation_check.sh`,
`stability_check.sh`, `ci_local.sh`, `real_aof_sample.sh`, `bench_table.py`, `plot_bench.py`, `stamp_parser_bench.sh`) | `results/` (all committed
evidence) | `Makefile`, `Dockerfile`, `.github/workflows/ci.yml`.
