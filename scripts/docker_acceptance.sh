#!/usr/bin/env bash
# Docker acceptance: build the image, smoke it with the official redis-cli, run `make docker-test`.
# Uses `make` directly when it exists; otherwise (e.g. Git Bash on Windows) runs make inside a throw-away
# docker:cli container that has the host's docker socket.
#   usage: scripts/docker_acceptance.sh      -> results/docker_acceptance.txt
set -uo pipefail
export MSYS_NO_PATHCONV=1
cd "$(dirname "$0")/.."
OUT=results/docker_acceptance.txt
mkdir -p results
status=0

{
  echo "Docker acceptance transcript"
  echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)   docker: $(docker version --format '{{.Server.Version}}')   run from: $(uname -s)"
  echo

  echo '$ make docker      (docker build -t redis-rs:latest .)'
  docker build -t redis-rs:latest . 2>&1 | tail -3 || status=1
  docker images redis-rs:latest --format 'image {{.Repository}}:{{.Tag}}  size {{.Size}}  base: gcr.io/distroless/cc-debian12:nonroot (no shell, uid 65532)'
  echo

  echo '$ make docker-smoke   (scripts/docker_smoke.sh: official redis-cli from redis:7 against the container)'
  bash scripts/docker_smoke.sh redis-rs:latest redis:7 || status=1
  echo

  if command -v make >/dev/null 2>&1; then
    echo '$ make docker-test'
    make docker-test > /tmp/docker-test.$$.log 2>&1; rc=$?
    grep -E '^test result|Running tests/' /tmp/docker-test.$$.log; rm -f /tmp/docker-test.$$.log
  else
    echo '$ make docker-test    (GNU make is not installed on this host: run inside a docker:cli container)'
    docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -v "$(pwd -W 2>/dev/null || pwd):/w" -w /w docker:cli sh -c \
      'apk add --no-cache make bash >/dev/null 2>&1; make docker-test > /tmp/dt.log 2>&1; rc=$?; grep -E "^test result|Running tests/" /tmp/dt.log; exit $rc'
    rc=$?
  fi
  echo "make docker-test exit status: $rc"
  [ "$rc" -eq 0 ] || status=1
  echo "leftover oracle containers/networks afterwards: $(docker ps -a --format '{{.Names}}' | grep -c '^redis-rs-oracle$') containers, $(docker network ls --format '{{.Name}}' | grep -c '^redis-rs-test-net$') networks"
} 2>&1 | tee "$OUT"
exit "${PIPESTATUS[0]}"
