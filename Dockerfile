# syntax=docker/dockerfile:1
#
#   docker build -t redis-rs .                       # minimal runtime image
#   docker build --target builder -t redis-rs-builder .   # builder with a warm test build (make docker-test)

ARG RUST_IMAGE=rust:1.99.1-slim-bookworm

FROM ${RUST_IMAGE} AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benches ./benches
COPY tests ./tests
RUN cargo build --release --locked
# Warm the debug test build so `make docker-test` only has to run the tests.
RUN cargo test --no-run --locked
# An empty, world-writable-by-nonroot data dir to copy into the distroless image.
RUN mkdir /data-skel

FROM gcr.io/distroless/cc-debian12:nonroot AS runtime
COPY --from=builder /app/target/release/redis-rs /usr/local/bin/redis-rs
COPY --from=builder --chown=65532:65532 /data-skel /data
WORKDIR /data
VOLUME /data
EXPOSE 6379
USER nonroot
ENTRYPOINT ["/usr/local/bin/redis-rs"]
# 0.0.0.0 so the published port is reachable; AOF on, in the volume.
CMD ["--bind", "0.0.0.0", "--appendonly", "yes", "--dir", "/data"]
