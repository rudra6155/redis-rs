#!/usr/bin/env bash
# Smoke test (CI, well under 30 s): start the release binary, PING / SET / GET over a raw socket
# (bash's /dev/tcp: no client library involved), then SIGINT and require a clean exit 0.
#   usage: scripts/smoke.sh [path/to/redis-rs]
set -euo pipefail
BIN="${1:-${CARGO_TARGET_DIR:-target}/release/redis-rs}"
[ -x "$BIN" ] || { echo "smoke: $BIN not found (run: make install)"; exit 2; }

OUT="$(mktemp)"; ERR="$(mktemp)"
PID=""
cleanup() { [ -n "$PID" ] && kill -9 "$PID" 2>/dev/null || true; rm -f "$OUT" "$ERR"; }
trap cleanup EXIT

"$BIN" --port 0 >"$OUT" 2>"$ERR" &
PID=$!

ADDR=""
for _ in $(seq 1 100); do
  ADDR="$(sed -n 's/^Ready to accept connections on //p' "$OUT")"
  [ -n "$ADDR" ] && break
  kill -0 "$PID" 2>/dev/null || { echo "smoke: server exited early:"; cat "$ERR"; exit 1; }
  sleep 0.1
done
[ -n "$ADDR" ] || { echo "smoke: server never became ready"; cat "$ERR"; exit 1; }

exec 3<>"/dev/tcp/${ADDR%:*}/${ADDR##*:}"
fail() { echo "smoke FAILED: $*"; exit 1; }

# check <raw request> <expected reply lines joined by '|' (CR stripped)> <number of lines to read>
check() {
  local req="$1" want="$2" n="$3" got="" line
  printf '%b' "$req" >&3
  for _ in $(seq 1 "$n"); do
    IFS= read -r -t 5 line <&3 || fail "timeout waiting for the reply to: $req"
    got+="${line%$'\r'}|"
  done
  [ "$got" = "$want" ] || fail "request '$req': expected '$want', got '$got'"
}

check '*1\r\n$4\r\nPING\r\n' '+PONG|' 1
check '*3\r\n$3\r\nSET\r\n$5\r\nsmoke\r\n$2\r\nok\r\n' '+OK|' 1
check '*2\r\n$3\r\nGET\r\n$5\r\nsmoke\r\n' '$2|ok|' 2
check '*2\r\n$3\r\nGET\r\n$7\r\nmissing\r\n' '$-1|' 1
exec 3>&-

kill -INT "$PID"
code=0
wait "$PID" || code=$?
PID=""
[ "$code" -eq 0 ] || { cat "$ERR"; fail "exit code $code after SIGINT (expected 0)"; }
echo "smoke OK: PING/SET/GET served on $ADDR, SIGINT exited 0"
