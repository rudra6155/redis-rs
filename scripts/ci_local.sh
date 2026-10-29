#!/usr/bin/env bash
# Run, locally and in order, exactly the commands in .github/workflows/ci.yml and record a transcript.
# This is NOT a GitHub-hosted run (that stays PENDING in CLAIMS.md until the repo is pushed); it proves
# the workflow's commands pass on a clean copy of the tree.
#   needs: cargo (+clippy), and for the oracle job either Docker or ORACLE_ADDR=host:port
#   usage: scripts/ci_local.sh        -> results/ci_local_run.txt
set -uo pipefail
cd "$(dirname "$0")/.."
OUT=results/ci_local_run.txt
mkdir -p results
BIN="${CARGO_TARGET_DIR:-target}/release/redis-rs"
failed=0

{
  echo "CI steps executed locally (mirror of .github/workflows/ci.yml)"
  echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)   rustc: $(rustc --version)   clippy: $(cargo clippy --version)"
  echo
} > "$OUT"

step() { # <label> <command...>
  local label="$1"; shift
  local log; log="$(mktemp)"
  local start; start=$(date +%s)
  if "$@" >"$log" 2>&1; then local verdict=PASS; else local verdict=FAIL; failed=$((failed+1)); fi
  printf '%-4s %-58s (%ss)\n' "$verdict" "$label" "$(( $(date +%s) - start ))" | tee -a "$OUT"
  tail -n "${TAIL:-2}" "$log" | sed 's/^/       /' | tee -a "$OUT"
  rm -f "$log"
}

echo "job build-lint-test" | tee -a "$OUT"
TAIL=1 step "build (release): cargo build --release --locked" cargo build --release --locked
TAIL=1 step "clippy: cargo clippy --all-targets --locked -- -D warnings" cargo clippy --all-targets --locked -- -D warnings
# count the tests that ran
nodocker() { make test-nodocker 2>&1 | tee /dev/stderr | grep -E '^test result' | awk '{p+=$4; f+=$6} END {print "tests passed=" p " failed=" f; exit (f>0)}'; }
TAIL=1 step "tests without Docker: make test-nodocker" bash -c "$(declare -f nodocker); nodocker 2>/dev/null"
TAIL=1 step "smoke (timeout 30): scripts/smoke.sh $BIN" timeout 30 bash scripts/smoke.sh "$BIN"

echo | tee -a "$OUT"
echo "job oracle (needs Docker or ORACLE_ADDR)" | tee -a "$OUT"
TAIL=3 step "oracle: cargo test --test protocol_oracle --test error_semantics" cargo test --locked --test protocol_oracle --test error_semantics
grep -E '^oracle |^SUMMARY' results/oracle_report.txt | sed 's/^/       /' | tee -a "$OUT"

echo | tee -a "$OUT"
echo "SUMMARY: $failed step(s) failed" | tee -a "$OUT"
[ "$failed" -eq 0 ]
