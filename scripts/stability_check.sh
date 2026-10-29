#!/usr/bin/env bash
# Flake hunt: run the whole test suite repeatedly, then repeatedly under CPU load, and record
# the pass/fail counts. Timing-based tests (the 150 ms sweeper check, SIGINT races, fsync
# counters) are where flakes hide. Needs cargo and either Docker or ORACLE_ADDR (the oracle tests run too).
#   usage: scripts/stability_check.sh [plain_runs=10] [loaded_runs=5]
#   writes results/stability_check.txt
set -uo pipefail
cd "$(dirname "$0")/.."
PLAIN="${1:-10}"; LOADED="${2:-5}"
OUT=results/stability_check.txt
LOGDIR="$(mktemp -d)"
trap 'rm -rf "$LOGDIR"' EXIT
mkdir -p results

cargo test --locked --no-run >/dev/null 2>&1 || { echo "build failed"; exit 1; }

tally() { # <log> -> "passed/failed"
  grep -hE '^test result' "$1" | awk '{p+=$4; f+=$6} END {printf "%d/%d", p, f}'
}

total_fail=0
{
  echo "redis-rs stability check"
  echo "run at : $(date -u +%Y-%m-%dT%H:%M:%SZ)   cores: $(nproc)   $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | sed 's/^ *//')"
  echo "what   : the full 'cargo test' (all test binaries, oracle included), $PLAIN plain runs then $LOADED runs"
  echo "         while $(( $(nproc) * 3 / 4 )) busy-loop processes compete for the CPUs. Format per run: passed/failed"
  echo
} > "$OUT"

echo -n "plain : " | tee -a "$OUT"
for i in $(seq 1 "$PLAIN"); do
  cargo test --locked >"$LOGDIR/p$i.log" 2>&1; rc=$?
  r="$(tally "$LOGDIR/p$i.log")"; [ "$rc" -ne 0 ] && total_fail=$((total_fail+1))
  echo -n "$r " | tee -a "$OUT"
done
echo | tee -a "$OUT"

# busy loops, each bounded by `timeout` so they cannot outlive the script
pids=()
for _ in $(seq 1 $(( $(nproc) * 3 / 4 ))); do
  timeout 600 sh -c 'while :; do :; done' &
  pids+=($!)
done
echo -n "loaded: " | tee -a "$OUT"
for i in $(seq 1 "$LOADED"); do
  cargo test --locked >"$LOGDIR/l$i.log" 2>&1; rc=$?
  r="$(tally "$LOGDIR/l$i.log")"; [ "$rc" -ne 0 ] && total_fail=$((total_fail+1))
  echo -n "$r " | tee -a "$OUT"
done
echo | tee -a "$OUT"
kill "${pids[@]}" 2>/dev/null || true

echo | tee -a "$OUT"
echo "SUMMARY: $((PLAIN + LOADED)) full runs, $total_fail failed run(s)" | tee -a "$OUT"
[ "$total_fail" -eq 0 ]
