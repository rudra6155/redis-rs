#!/usr/bin/env bash
# Start the redis-rs image, drive it with the OFFICIAL redis-cli (from the redis image, joined to the
# container's network namespace), then stop it. Proves "the Docker image serves PING".
#   usage: scripts/docker_smoke.sh [redis-rs image] [redis image]
set -euo pipefail
export MSYS_NO_PATHCONV=1
IMAGE="${1:-redis-rs:latest}"
REDIS_IMAGE="${2:-redis:7}"
NAME="redis-rs-smoke-$$"
cleanup() { docker rm -f "$NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT

docker run -d --name "$NAME" "$IMAGE" >/dev/null
cli() { docker run --rm --network "container:$NAME" "$REDIS_IMAGE" redis-cli -h 127.0.0.1 "$@"; }

ready=0
for _ in $(seq 1 50); do
  [ "$(cli PING 2>/dev/null || true)" = "PONG" ] && ready=1 && break
  sleep 0.2
done
[ "$ready" = 1 ] || { docker logs "$NAME" 2>&1 | tail -n 20; echo "docker smoke FAILED: no PONG"; exit 1; }

[ "$(cli SET smoke ok)" = "OK" ] || { echo "docker smoke FAILED: SET"; exit 1; }
[ "$(cli GET smoke)" = "ok" ] || { echo "docker smoke FAILED: GET"; exit 1; }
echo "docker image $IMAGE serves PING/SET/GET (checked with redis-cli from $REDIS_IMAGE)"
