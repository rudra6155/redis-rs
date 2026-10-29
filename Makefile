# redis-rs - see README.md. Everything here assumes a Linux-like shell.
#
#   make install          cargo build --release
#   make test             all tests; includes protocol_oracle, which needs Docker (starts redis:7)
#   make test-nodocker    everything except the oracle tests (for CI without Docker-in-Docker)
#   make bench-vs-redis   redis:7 vs redis-rs with the official redis-benchmark -> results/
#   make docker           build the minimal runtime image
#   make docker-test      run the full test suite inside a container (redis:7 oracle as a sibling container)

SHELL        := bash
.SHELLFLAGS  := -o pipefail -c

CARGO        ?= cargo
REDIS_IMAGE  ?= redis:7
IMAGE        ?= redis-rs:latest
BIN          ?= $(or $(CARGO_TARGET_DIR),target)/release/redis-rs
NET          := redis-rs-test-net

.PHONY: help install build test test-nodocker clippy fmt-check lint smoke \
        bench-parser bench-vs-redis oracle-diff aof-sample mutation-check stability-check \
        docker docker-smoke docker-test clean

help:
	@sed -n '3,9p' Makefile

install build:
	$(CARGO) build --release --locked

# The oracle tests (protocol_oracle, error_semantics::oracle_*) start redis:7 with Docker, or use
# ORACLE_ADDR=host:port of a *disposable* redis:7. They fail (never skip) when neither exists.
test:
	@command -v docker >/dev/null 2>&1 || [ -n "$$ORACLE_ADDR" ] || { \
	  echo "make test needs Docker for the protocol_oracle tests (or ORACLE_ADDR=host:port)."; \
	  echo "Use 'make test-nodocker' to run everything else."; exit 1; }
	$(CARGO) test --locked

# Every oracle test has "oracle" in its name.
test-nodocker:
	$(CARGO) test --locked -- --skip oracle

clippy lint:
	$(CARGO) clippy --all-targets --locked -- -D warnings

fmt-check:
	$(CARGO) fmt --check

# ~10 s: start the release binary, PING / SET / GET over a raw socket, SIGINT, expect exit 0.
smoke: install
	bash scripts/smoke.sh $(BIN)

# Criterion microbenchmarks (parser, store, command path; no I/O) -> results/parser_bench.txt
bench-parser:
	@mkdir -p results
	raw=$$(mktemp); \
	$(CARGO) bench --locked --bench ops -- --noplot 2>&1 | tee $$raw; \
	bash scripts/stamp_parser_bench.sh $$raw results/parser_bench.txt; rm -f $$raw

# redis-benchmark (official tool, from the redis image) against redis:7 and redis-rs, same machine,
# same virtual network namespace. Writes results/bench_vs_redis.csv, p99_latency.csv, hardware.json, png.
bench-vs-redis:
	bash scripts/bench_vs_redis.sh

oracle-diff:
	bash scripts/oracle_diff.sh

aof-sample:
	bash scripts/real_aof_sample.sh

# Inject deliberate bugs, expect the named tests to fail (needs cargo only) -> results/mutation_report.txt
mutation-check:
	bash scripts/mutation_check.sh

# Run the whole suite repeatedly, plain and under CPU load -> results/stability_check.txt
stability-check:
	bash scripts/stability_check.sh

docker:
	docker build -t $(IMAGE) .

# Start the image, ping it with the official redis-cli, stop it.
docker-smoke: docker
	bash scripts/docker_smoke.sh $(IMAGE) $(REDIS_IMAGE)

# Full test suite inside a builder container; redis:7 runs as a sibling container on a private network.
docker-test:
	docker build --target builder -t redis-rs-builder:latest .
	-docker rm -f redis-rs-oracle >/dev/null 2>&1
	-docker network rm $(NET) >/dev/null 2>&1
	docker network create $(NET) >/dev/null
	docker run -d --rm --name redis-rs-oracle --network $(NET) $(REDIS_IMAGE) >/dev/null
	docker run --rm --network $(NET) -e ORACLE_ADDR=redis-rs-oracle:6379 redis-rs-builder:latest \
	  cargo test --locked; status=$$?; \
	  docker rm -f redis-rs-oracle >/dev/null 2>&1; docker network rm $(NET) >/dev/null 2>&1; exit $$status

clean:
	$(CARGO) clean
