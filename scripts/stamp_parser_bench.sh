#!/usr/bin/env bash
# Turn raw criterion output into results/parser_bench.txt with an environment header.
#   usage: scripts/stamp_parser_bench.sh <raw criterion output> <out file>
set -euo pipefail
raw="$1"; out="$2"
{
  echo "redis-rs criterion microbenchmarks (benches/ops.rs): NO I/O, no network."
  echo "Isolates parser / store / command-path cost from network cost."
  echo "run at : $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "cpu    : $(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2 | sed 's/^ *//' || echo unknown)"
  echo "rustc  : $(rustc --version)"
  echo "command: cargo bench --bench ops -- --noplot   (release profile; time = [low estimate high])"
  echo
  # Everything before the first "group/benchmark" line is cargo's compile output; criterion's own
  # progress and outlier lines are dropped, the time / thrpt result lines are kept.
  sed 's/\x1b\[[0-9;]*[A-Za-z]//g' "$raw" \
    | awk 'f || /^[a-z_]+\//{f=1; print}' \
    | grep -vE '^(Benchmarking|Warming up|Gnuplot not found|Found [0-9]+ outliers|  [0-9]+ \(|\s*$)' || true
} > "$out"
cat "$out"
