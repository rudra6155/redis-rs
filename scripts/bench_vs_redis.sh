#!/usr/bin/env bash
# redis:7 vs redis-rs with the OFFICIAL redis-benchmark (taken from the redis image, so the
# "your client is the bottleneck" objection does not apply: the client is the reference
# implementation's own).
#
#  * both servers run in containers; each benchmark run is a throw-away container that joins the
#    *network namespace of the server under test* (`--network container:NAME`), so both servers
#    are hit over the same kind of loopback connection (127.0.0.1) on the same machine
#  * config (per brief): -n 100000 -c 50 -t set,get -q, RUNS times; a second run per iteration
#    with --csv for p50/p99. NOTE: redis-benchmark has no `--latency` flag (that is redis-cli);
#    --csv is its percentile output (p50/p95/p99 since Redis 7).
#  * supplementary suites (same columns): `-r 100000` random keys, and `--threads 4` for the client
#  * one warm-up run per server is discarded; runs alternate between the servers; medians reported
#  * reference server: `redis-server --save "" --appendonly no` (pure in-memory, like redis-rs
#    started without --appendonly); redis-rs: default flags, AOF off
#  * if the reference server cannot be started -> results/BENCH_BLOCKED.txt, nothing else is
#    written (an in-process HashMap is never substituted for Redis)
#  * absurd numbers (>10M ops/s, or p99 < p50) -> results/bench_anomaly.txt, no CSV is published
#
# Env: RUNS (5)  N (100000)  C (50)  REDIS_IMAGE (redis:7)  MINE_IMAGE (redis-rs:bench)
set -uo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash on Windows must not rewrite container paths
cd "$(dirname "$0")/.."
RESULTS=results
mkdir -p "$RESULTS"
REDIS_IMAGE="${REDIS_IMAGE:-redis:7}"
MINE_IMAGE="${MINE_IMAGE:-redis-rs:bench}"
RUNS="${RUNS:-5}"; N="${N:-100000}"; C="${C:-50}"
REF=rrs-bench-ref; MINE=rrs-bench-mine
TMP="$(mktemp -d)"

blocked() {
  printf '%s\n' "$1" > "$RESULTS/BENCH_BLOCKED.txt"
  echo "BLOCKED: $1" >&2
  exit 2
}
cleanup() { docker rm -f "$REF" "$MINE" >/dev/null 2>&1 || true; rm -rf "$TMP"; }
trap cleanup EXIT

command -v docker >/dev/null 2>&1 || blocked "docker CLI not found on PATH; the reference redis-server ($REDIS_IMAGE) cannot be started."
docker info >/dev/null 2>&1 || blocked "docker daemon is not reachable ($(docker info 2>&1 | tail -n1)); the reference redis-server ($REDIS_IMAGE) cannot be started."
if ! docker pull -q "$REDIS_IMAGE" >/dev/null 2>&1 && ! docker image inspect "$REDIS_IMAGE" >/dev/null 2>&1; then
  blocked "cannot pull $REDIS_IMAGE and there is no local copy; the reference redis-server cannot be started."
fi
rm -f "$RESULTS/BENCH_BLOCKED.txt" "$RESULTS/bench_anomaly.txt"

# what else is running on the Docker VM right now: benchmark numbers are only interpretable next to this
{
  echo "Benchmark environment snapshot, taken just before the runs ($(date -u +%Y-%m-%dT%H:%M:%SZ))"
  docker ps --format 'table {{.Names}}	{{.Image}}	{{.Status}}'
  echo
  docker stats --no-stream --format 'table {{.Name}}	{{.CPUPerc}}	{{.MemUsage}}'
} > "$RESULTS/bench_environment.txt" 2>&1

echo ">> building $MINE_IMAGE"
docker build -q --target runtime -t "$MINE_IMAGE" . >/dev/null || blocked "docker build of redis-rs failed"

cleanup_containers() { docker rm -f "$REF" "$MINE" >/dev/null 2>&1 || true; }
cleanup_containers
docker run -d --name "$REF" "$REDIS_IMAGE" redis-server --save "" --appendonly no >/dev/null \
  || blocked "could not start the reference container from $REDIS_IMAGE"
docker run -d --name "$MINE" "$MINE_IMAGE" --bind 0.0.0.0 --appendonly no >/dev/null \
  || blocked "could not start the redis-rs container from $MINE_IMAGE"

cli() { docker run --rm --network "container:$1" "$REDIS_IMAGE" redis-cli -h 127.0.0.1 "${@:2}"; }
for c in "$REF" "$MINE"; do
  ok=0
  for _ in $(seq 1 60); do [ "$(cli "$c" PING 2>/dev/null)" = "PONG" ] && ok=1 && break; sleep 0.25; done
  [ "$ok" = 1 ] || { docker logs "$c" 2>&1 | tail -n 5; blocked "server in container $c never answered PING"; }
done

bench() { # <container> <redis-benchmark args...>
  docker run --rm --network "container:$1" "$REDIS_IMAGE" redis-benchmark -h 127.0.0.1 -p 6379 "${@:2}" 2>&1 | tr '\r' '\n'
}

# suite <label> <extra redis-benchmark args...>   -> $TMP/<label>.{rps,csv}.<server>
suite() {
  local label="$1"; shift
  local -a extra=("$@")
  echo ">> suite '$label': warm-up"
  bench "$REF" -n "$N" -c "$C" -t set,get -q "${extra[@]}" >/dev/null
  bench "$MINE" -n "$N" -c "$C" -t set,get -q "${extra[@]}" >/dev/null
  for run in $(seq 1 "$RUNS"); do
    for pair in "redis:$REF" "redis-rs:$MINE"; do
      local srv="${pair%%:*}" cont="${pair#*:}"
      local out; out="$(bench "$cont" -n "$N" -c "$C" -t set,get -q "${extra[@]}")"
      for op in SET GET; do
        local rps; rps="$(grep -E "^ *$op: [0-9.]+ requests per second" <<<"$out" | tail -n1 | sed -E 's/^ *[A-Z]+: ([0-9.]+) .*/\1/')"
        [ -n "$rps" ] || { echo "no $op throughput parsed for $srv:"; echo "$out" | tail -n 8; blocked "redis-benchmark produced no parsable output against $srv"; }
        echo "$rps" >> "$TMP/$label.rps.$srv.$op"
      done
      bench "$cont" -n "$N" -c "$C" -t set,get --csv "${extra[@]}" | tr -d '"' | grep -E '^(SET|GET),' \
        | awk -F, -v run="$run" -v srv="$srv" '{print srv","$1","run","$2","$3","$4","$5","$6","$7","$8}' >> "$TMP/$label.csv"
      echo "   run $run/$RUNS $srv done"
    done
  done
}

median() { sort -g | awk '{a[NR]=$1} END{ if (NR==0) print "NA"; else if (NR%2) print a[(NR+1)/2]; else printf "%.3f\n", (a[NR/2]+a[NR/2+1])/2 }'; }

suite main
suite random_keys -r 100000
# 4 benchmark threads: shows whether the default single-threaded client was the ceiling
suite threads4 --threads 4

# ---- anomaly gate ----
anom=""
for f in "$TMP"/*.rps.*; do
  while read -r v; do
    awk -v v="$v" 'BEGIN{exit !(v+0 > 10000000)}' && anom+="throughput $v ops/s (> 10M) in $(basename "$f")"$'\n'
  done < "$f"
done
for f in "$TMP"/main.csv "$TMP"/random_keys.csv "$TMP"/threads4.csv; do
  # columns: server,op,run,rps,avg,min,p50,p95,p99,max
  anom+="$(awk -F, '$9+0 < $7+0 {print "p99 < p50 in "FILENAME": "$0}' "$f")"
done
if [ -n "${anom//[$'\n ']/}" ]; then
  { echo "redis-benchmark reported implausible numbers; treated as a harness bug, NOT published:"; echo "$anom"; } > "$RESULTS/bench_anomaly.txt"
  echo "ANOMALY: see $RESULTS/bench_anomaly.txt" >&2
  exit 3
fi

# ---- environment ----
RUST_IMAGE="$(sed -n 's/^ARG RUST_IMAGE=//p' Dockerfile | head -n1)"
host_cpu="$(docker run --rm "$REDIS_IMAGE" sh -c "grep -m1 'model name' /proc/cpuinfo | cut -d: -f2" | sed 's/^ *//')"
cores="$(docker info --format '{{.NCPU}}')"
ram_gb="$(docker info --format '{{.MemTotal}}' | awk '{printf "%.1f", $1/1024/1024/1024}')"
kernel="$(docker info --format '{{.KernelVersion}}')"
os_desc="$(docker info --format '{{.OperatingSystem}}' | tr ',' ';') (Linux VM, kernel $kernel); script run from $(uname -s)"
rust_version="$(docker run --rm "$RUST_IMAGE" rustc --version 2>/dev/null | head -n1)"
cargo_version="$(docker run --rm "$RUST_IMAGE" cargo --version 2>/dev/null | head -n1)"
redis_version="$(docker exec "$REF" redis-server --version | sed -E 's/.* v=([0-9.]+) .*/\1/')"
redis_digest="$(docker image inspect --format '{{index .RepoDigests 0}}' "$REDIS_IMAGE" 2>/dev/null || echo unknown)"
mine_version="$(docker run --rm "$MINE_IMAGE" --version | awk '{print $2}')"
now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

cat > "$RESULTS/hardware.json" <<EOF
{
  "cpu_model": "$host_cpu",
  "cores": $cores,
  "ram_gb": $ram_gb,
  "os": "$os_desc",
  "kernel": "$kernel",
  "rust_version": "$rust_version",
  "cargo_version": "$cargo_version",
  "redis_version": "$redis_version",
  "redis_image_tag": "$REDIS_IMAGE ($redis_digest)",
  "run_datetime": "$now"
}
EOF

write_csv() { # <label> <outfile>
  local label="$1" out="$2"
  echo "server,version,op,n_ops,clients,ops_per_sec,p50_ms,p99_ms,host_cpu,host_ram_gb,os" > "$out"
  for srv in redis redis-rs; do
    local ver; [ "$srv" = redis ] && ver="$redis_version" || ver="$mine_version"
    for op in SET GET; do
      local rps p50 p99
      rps="$(median < "$TMP/$label.rps.$srv.$op")"
      p50="$(awk -F, -v s="$srv" -v o="$op" '$1==s && $2==o {print $7}' "$TMP/$label.csv" | median)"
      p99="$(awk -F, -v s="$srv" -v o="$op" '$1==s && $2==o {print $9}' "$TMP/$label.csv" | median)"
      echo "$srv,$ver,$op,$N,$C,$rps,$p50,$p99,\"$host_cpu\",$ram_gb,\"$os_desc\"" >> "$out"
    done
  done
}
write_csv main "$RESULTS/bench_vs_redis.csv"
write_csv random_keys "$RESULTS/bench_vs_redis_random_keys.csv"
write_csv threads4 "$RESULTS/bench_vs_redis_threads4.csv"

# every individual csv-mode run, for the p99 distribution
{ echo "server,op,run,rps,avg_ms,min_ms,p50_ms,p95_ms,p99_ms,max_ms"; cat "$TMP/main.csv"; } > "$RESULTS/p99_latency.csv"

# ---- chart ----
if python3 -c 'import matplotlib' >/dev/null 2>&1; then
  python3 scripts/plot_bench.py "$RESULTS/bench_vs_redis.csv" "$RESULTS/bench_vs_redis.png"
else
  echo ">> matplotlib not on the host; rendering the chart in a python container"
  cid="$(docker create python:3.12-slim sh -c 'pip install -q --no-cache-dir matplotlib >/dev/null 2>&1 && python /plot.py /in.csv /out.png')"
  docker cp scripts/plot_bench.py "$cid:/plot.py" && docker cp "$RESULTS/bench_vs_redis.csv" "$cid:/in.csv" \
    && docker start -a "$cid" && docker cp "$cid:/out.png" "$RESULTS/bench_vs_redis.png"
  docker rm -f "$cid" >/dev/null 2>&1
fi

echo; echo "== $RESULTS/bench_vs_redis.csv"; cat "$RESULTS/bench_vs_redis.csv"
echo; echo "== $RESULTS/hardware.json"; cat "$RESULTS/hardware.json"
