#!/usr/bin/env bash
# Simulated fresh clone: copy the tree WITHOUT target/ (and .git) into a scratch directory, point cargo at an
# empty target dir (cold build), run `make install` and `make test`, record timings.
# The crates.io download cache is whatever the machine already has (state that in the transcript).
# `make test` needs Docker for the oracle tests, or ORACLE_ADDR=host:port of a disposable redis:7.
#   usage: scripts/fresh_clone.sh      -> results/fresh_clone_run.txt
set -uo pipefail
SRC="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
OUT="$SRC/results/fresh_clone_run.txt"
mkdir -p "$SRC/results"

(cd "$SRC" && tar --exclude=./target --exclude=./.git -cf - .) | tar -xf - -C "$WORK/"
export CARGO_TARGET_DIR="$WORK/cold-target"
cd "$WORK"

secs() { python3 -c "import sys; print(f'{float(sys.argv[2]) - float(sys.argv[1]):.1f}')" "$1" "$2"; }
{
  echo "Fresh-clone acceptance transcript (simulated clone: tree copied without target/, cold CARGO_TARGET_DIR)"
  echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)   rustc: $(rustc --version)   cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | sed 's/^ *//')"
  if [ -n "${ORACLE_ADDR:-}" ]; then
    echo "oracle: ORACLE_ADDR=$ORACLE_ADDR (a sibling redis:7 container; no docker CLI inside this container)"
  else
    echo "oracle: started by the tests with 'docker run redis:7'"
  fi
  echo "crates.io download cache: warm (dependencies were already downloaded on this machine)"
  echo

  echo '$ make install'
  s=$(date +%s.%N); make install 2>&1 | tail -2; e=$(date +%s.%N)
  echo "wall: $(secs "$s" "$e") s"
  echo

  echo '$ make test      # includes protocol_oracle + error_semantics oracle test'
  s=$(date +%s.%N); make test 2>&1 | grep -E '^(test result|error|FAILED)|failed|Running (unittests|tests)' | sed -E 's#\(/[^)]*/#(#'; e=$(date +%s.%N)
  echo "wall: $(secs "$s" "$e") s   (budget: 180 s)"
} 2>&1 | tee "$OUT"
