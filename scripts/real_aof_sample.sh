#!/usr/bin/env bash
# Capture what a REAL redis:7 writes to its AOF for our command set, so the AOF format claim in
# SPEC.md is checked against the reference rather than remembered.
# Output: results/real_redis_aof_sample.txt, rendered INSIDE the container with `cat -A` (CR = ^M,
# LF = $) so no host-side newline conversion (e.g. Git Bash on Windows) can hide the CRLF framing.
set -euo pipefail
export MSYS_NO_PATHCONV=1
cd "$(dirname "$0")/.."
IMAGE="${REDIS_IMAGE:-redis:7}"
NAME="redis-aof-sample-$$"
mkdir -p results
cleanup() { docker rm -f "$NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT

docker run -d --name "$NAME" "$IMAGE" redis-server --appendonly yes --appendfsync always >/dev/null
for _ in $(seq 1 50); do docker exec "$NAME" redis-cli ping >/dev/null 2>&1 && break; sleep 0.2; done
version="$(docker exec "$NAME" redis-server --version | sed -E 's/.* v=([0-9.]+) .*/\1/')"

CMDS=(
  "SET k v"
  "SET e v EX 100"
  "SET p v PX 5000"
  "EXPIRE k 100"
  "INCR n"
  "HSET h f v g w"
  "HDEL h f"
  "DEL p"
  "EXPIRE e 0"
  "GET k"
)
for c in "${CMDS[@]}"; do docker exec "$NAME" redis-cli $c >/dev/null; done

{
  echo "# AOF written by a real redis-server $version (--appendonly yes --appendfsync always)"
  echo "# commands sent through redis-cli, in order:"
  printf '#   %s\n' "${CMDS[@]}"
  echo "# file: appendonlydir/*.incr.aof, shown with cat -A: ^M = CR, $ = LF (GET is not journaled)"
  echo "# note: EX/PX were rewritten to absolute SET ... PXAT <unix-ms>, EXPIRE to PEXPIREAT, and an"
  echo "#       EXPIRE with a non-positive time to DEL. redis-rs journals exactly these forms."
  echo
  docker exec "$NAME" sh -c 'cat -A /data/appendonlydir/*.incr.aof'
} > results/real_redis_aof_sample.txt
cat results/real_redis_aof_sample.txt
