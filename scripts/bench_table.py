#!/usr/bin/env python3
"""Print the README benchmark tables (markdown) from the committed CSV files.

usage: python3 scripts/bench_table.py            (run from the repo root)
"""
import csv
import json
import sys


def load(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def table(path, title):
    rows = load(path)
    by = {(r["server"], r["op"]): r for r in rows}
    out = [f"**{title}** (`{path}`)", "",
           "| op | redis ops/s | redis-rs ops/s | redis-rs / redis | redis p50 / p99 (ms) | redis-rs p50 / p99 (ms) |",
           "|---|---:|---:|---:|---:|---:|"]
    for op in ("SET", "GET"):
        a, b = by[("redis", op)], by[("redis-rs", op)]
        ra, rb = float(a["ops_per_sec"]), float(b["ops_per_sec"])
        out.append(
            f"| {op} | {ra:,.0f} | {rb:,.0f} | {rb / ra:.2f}x | {a['p50_ms']} / {a['p99_ms']} | {b['p50_ms']} / {b['p99_ms']} |"
        )
    return "\n".join(out)


if __name__ == "__main__":
    hw = json.load(open("results/hardware.json"))
    print(f"redis {hw['redis_version']} ({hw['redis_image_tag']}) vs redis-rs; {hw['cpu_model']}, "
          f"{hw['cores']} cores, {hw['ram_gb']} GB (Docker VM), run {hw['run_datetime']}\n")
    print(table("results/bench_vs_redis.csv", "Headline: -n 100000 -c 50 -t set,get (one key, 1 client thread)"))
    print()
    print(table("results/bench_vs_redis_random_keys.csv", "Supplementary: same, `-r 100000` (random keys)"))
    print()
    print(table("results/bench_vs_redis_threads4.csv", "Supplementary: same, `--threads 4` (4 client threads)"))
    sys.exit(0)
