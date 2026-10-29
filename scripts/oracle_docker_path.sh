#!/usr/bin/env bash
# Exercise the "tests start redis:7 themselves with `docker run`" path (what `make test` does on a developer
# machine) from inside a container that has cargo AND the docker CLI, on the host network, and prove it
# leaves no container behind. Also (re)writes results/oracle_report.txt from that path.
#
# Why a container: the author's host had no Rust toolchain. Inside a host-network container on Docker Desktop
# the published 127.0.0.1:<port> is not reachable, so this run also exercises the harness' fallback to the
# container's bridge IP (see tests/common/mod.rs).
#   usage: scripts/oracle_docker_path.sh      -> results/oracle_docker_path.txt, results/oracle_report.txt
set -uo pipefail
export MSYS_NO_PATHCONV=1
cd "$(dirname "$0")/.."
OUT=results/oracle_docker_path.txt
RUST_IMAGE="$(sed -n 's/^ARG RUST_IMAGE=//p' Dockerfile | head -n1)"
HELPER=redis-rs-dockerhost-test
PROJ="$(pwd -W 2>/dev/null || pwd)"

docker build -q -t "$HELPER" - >/dev/null <<EOF
FROM ${RUST_IMAGE}
COPY --from=docker:cli /usr/local/bin/docker /usr/local/bin/docker
EOF

before="$(docker ps -q | wc -l | tr -d ' ')"
{
  echo "Oracle tests via the 'docker run redis:7' path (ORACLE_ADDR unset)"
  echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)   helper image: $RUST_IMAGE + docker CLI, --network host, docker socket mounted"
  echo
  echo '$ cargo test --locked --test protocol_oracle --test error_semantics     (ORACLE_ADDR unset)'
  docker run --rm --network host -v /var/run/docker.sock:/var/run/docker.sock \
    -v "$PROJ:/work" -v redisrs-cargo-registry:/usr/local/cargo/registry -v redisrs-target-dh:/cargo-target \
    -e CARGO_TARGET_DIR=/cargo-target -w /work "$HELPER" \
    bash -c 'unset ORACLE_ADDR; cargo test --locked --test protocol_oracle --test error_semantics 2>&1 | grep -E "^test |test result|panicked"; echo "cargo exit status: ${PIPESTATUS[0]}"'
  after="$(docker ps -q | wc -l | tr -d ' ')"
  echo
  echo "running containers before the run: $before, after: $after  ($([ "$before" = "$after" ] && echo 'nothing leaked' || echo 'LEAK'))"
  grep -E '^oracle |^SUMMARY' results/oracle_report.txt
} 2>&1 | tee "$OUT"
grep -q 'nothing leaked' "$OUT" && grep -q ' 0 FAIL' "$OUT"
