#!/usr/bin/env bash
# Mutation check: prove the tests can fail.
#
# For each mutation, copy the sources to a scratch dir, inject ONE deliberate bug,
# and run the test(s) that are supposed to catch it. A mutation that the tests do
# not catch ("SURVIVED") is a hole in the suite. Also runs the negative control for
# the `clippy::await_holding_lock` rule: a file that holds a std::sync guard across
# an .await must make clippy fail.
#
# Needs only cargo (no Docker). Writes results/mutation_report.txt.
#   usage: scripts/mutation_check.sh
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d)"
export CARGO_TARGET_DIR="${MUT_TARGET:-/tmp/redis-rs-mutation-target}"
REPORT="$ROOT/results/mutation_report.txt"
mkdir -p "$ROOT/results"
trap 'rm -rf "$WORK"' EXIT

cp -r "$ROOT/Cargo.toml" "$ROOT/Cargo.lock" "$ROOT/src" "$ROOT/tests" "$ROOT/benches" "$WORK/"

{
  echo "redis-rs mutation check"
  echo "run at: $(date -u +%Y-%m-%dT%H:%M:%SZ)   rustc: $(rustc --version)"
  echo "each row: one injected bug -> the tests that must fail"
  echo
} > "$REPORT"

survived=0
caught=0

reset() {
  rm -rf "$WORK/src" "$WORK/tests"
  cp -r "$ROOT/src" "$ROOT/tests" "$WORK/"
}

# mutate <name> <file> <perl -0 substitution> <cargo test args...>
mutate() {
  local name="$1" file="$2" expr="$3"; shift 3
  reset
  perl -0pi -e "$expr" "$WORK/$file"
  if cmp -s "$ROOT/$file" "$WORK/$file"; then
    echo "!! mutation '$name' did not change $file (stale pattern)"; echo "ERROR    $name: pattern did not apply" >> "$REPORT"; survived=$((survived+1)); return
  fi
  local log="$WORK/log.txt" rc=0
  for t in "$@"; do
    # shellcheck disable=SC2086
    (cd "$WORK" && cargo test --locked $t) > "$log" 2>&1 && continue
    rc=1
    break
  done
  if [ $rc -ne 0 ]; then
    local failing; failing=$(grep -E '^test .* FAILED$|^error(\[|:)' "$log" | head -3 | tr '\n' ';')
    printf 'CAUGHT   %-34s by: %s\n' "$name" "${failing:-compile error}" | tee -a "$REPORT"
    caught=$((caught+1))
  else
    printf 'SURVIVED %-34s (tests: %s)\n' "$name" "$*" | tee -a "$REPORT"
    survived=$((survived+1))
  fi
}

mutate "ttl-truncates-instead-of-rounds" src/store.rs \
  's/\(t - now \+ 500\) \/ 1000/(t - now) \/ 1000/' \
  "--test expiry ttl_reports"

mutate "arity-error-name-not-lowercase" src/command.rs \
  's/for \x27\{name\}\x27 command"\)\)/for \x27{}\x27 command", name.to_uppercase()))/' \
  "--test error_semantics error_strings_are_exact"

mutate "get-on-hash-returns-nil-not-wrongtype" src/store.rs \
  's/Value::Hash\(_\) => Err\(StoreError::WrongType\),/Value::Hash(_) => Ok(None),/' \
  "--test error_semantics error_strings_are_exact"

mutate "lazy-expiry-not-journaled" src/store.rs \
  's/(if expired \{\n\s*self\.remove_entry\(key\);\n)\s*self\.log\(&\[b"DEL", key\]\);\n/$1/' \
  "--test persistence_crash replay_matches_runtime"

mutate "replay-treats-deadline-as-relative" src/command.rs \
  's/Some\(t\) if t > 0 => Some\(t as u64\),/Some(t) if t > 0 => Some(t as u64 + crate::expiry::now_ms()),/' \
  "--test persistence_crash px_key_does_not_resurrect"

mutate "sweeper-does-nothing" src/expiry.rs \
  's/store\.sweep_cycle\(now_ms\(\)\);/let _ = \&store;/' \
  "--test expiry sweeper_removes"

mutate "everysec-fsyncs-inline" src/aof.rs \
  's/FsyncPolicy::Everysec \| FsyncPolicy::No => dirty\.store\(true, Ordering::Release\),/FsyncPolicy::Everysec | FsyncPolicy::No => { let _ = file.sync_data(); stats.fsyncs.fetch_add(1, Ordering::Relaxed); dirty.store(true, Ordering::Release) }/' \
  "--test persistence_crash fsync_policy_is_observable_everysec"

mutate "always-never-fsyncs" src/aof.rs \
  's/FsyncPolicy::Always => \{/FsyncPolicy::Always if false => {/; s/FsyncPolicy::Everysec \| FsyncPolicy::No => dirty\.store/_ => dirty.store/' \
  "--test persistence_crash fsync_policy_is_observable_always"

mutate "torn-tail-not-truncated" src/aof.rs \
  's/f\.set_len\(offset\)\?;/let _ = \&f;/' \
  "--test persistence_crash a_torn_final_command"

mutate "bulk-terminator-not-checked" src/resp.rs \
  's/if buf\[end\] != b\x27\\r\x27 \|\| buf\[end \+ 1\] != b\x27\\n\x27 \{\n\s*return Err\(ProtocolError::BadBulkTerminator\);\n\s*\}//' \
  "--test resp_unit bulk_terminator"

mutate "hgetall-loses-insertion-order" src/store.rs \
  's/all\.sort_unstable_by_key\(\|\(seq, _, _\)\| \*seq\);//' \
  "--test expiry store_agrees"

mutate "shutdown-never-tells-connections-to-stop" src/server.rs \
  's/let _ = stop_tx\.send\(true\);//' \
  "--test persistence_crash shutdown_closes_idle"

# Signal handlers installed AFTER the "Ready" line (the sleep just widens a window that is otherwise microseconds).
mutate "signals-installed-after-ready-line" src/main.rs \
  's/(        \/\/ Handlers first, "Ready" second: see ShutdownSignals\.\n)(        let signals = match ShutdownSignals::install\(\) \{.*?\n        \};\n)(.*?let _ = std::io::stdout\(\)\.flush\(\);\n)/$3        std::thread::sleep(std::time::Duration::from_millis(100));\n$2/s' \
  "--test persistence_crash sigint_immediately_after_ready"

# INCR split into read-then-write under two separate lock scopes: the classic lost-update bug.
cat > "$WORK/incr_repl.rs" <<'EOF'
fn cmd_incr(ctx: &mut Ctx<'_>, args: &[&[u8]]) -> Frame {
    let key = args[1];
    let cur = ctx.shard(key, |sh, now| sh.get(key, now));
    let n = match cur {
        Ok(Some(v)) => parse_i64(&v).unwrap_or(0) + 1,
        Ok(None) => 1,
        Err(e) => return store_error(e),
    };
    ctx.shard(key, |sh, _| sh.set(key, n.to_string().as_bytes(), None));
    Frame::Integer(n)
}
EOF
REPL="$WORK/incr_repl.rs" mutate "incr-uses-two-lock-scopes" src/command.rs \
  'BEGIN { open F, "<", $ENV{REPL}; local $/; $r = <F>; close F } s/fn cmd_incr\(.*?\n\}\n/$r/s' \
  "--test concurrency fifty_tasks"

# ---- negative control for the lint ----
reset
cat > "$WORK/src/bad.rs" <<'EOF'
//! Deliberately wrong: holds a std::sync guard across an .await.
pub async fn bad(m: &std::sync::Mutex<u32>) {
    let guard = m.lock().unwrap();
    tokio::task::yield_now().await;
    drop(guard);
}
EOF
echo "pub mod bad;" >> "$WORK/src/lib.rs"
if (cd "$WORK" && cargo clippy --locked --lib) > "$WORK/clippy.log" 2>&1; then
  echo "SURVIVED clippy-await-holding-lock-negative-control (clippy passed on a guard held across .await)" | tee -a "$REPORT"
  survived=$((survived+1))
elif grep -q "await_holding_lock" "$WORK/clippy.log"; then
  echo "CAUGHT   clippy::await_holding_lock denies a guard held across .await" | tee -a "$REPORT"
  grep -A3 "await_holding_lock" "$WORK/clippy.log" | head -6 >> "$REPORT"
  caught=$((caught+1))
else
  echo "ERROR    clippy failed for a different reason:"; tail -20 "$WORK/clippy.log"
  survived=$((survived+1))
fi

echo | tee -a "$REPORT"
echo "SUMMARY: $caught caught, $survived survived/errored" | tee -a "$REPORT"
[ "$survived" -eq 0 ]
