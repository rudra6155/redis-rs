#!/usr/bin/env python3
"""Render results/bench_vs_redis.png from results/bench_vs_redis.csv.

usage: plot_bench.py <bench_vs_redis.csv> <out.png>

Two panels (throughput, p99 latency) instead of one dual-axis chart. Colours are
fixed per server (blue = real redis, orange = redis-rs), validated for colour-blind
separation; every bar carries its value as a direct label.
"""
import csv
import sys
import textwrap

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

SURFACE, INK, INK2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
COLORS = {"redis": "#2a78d6", "redis-rs": "#eb6834"}

rows = list(csv.DictReader(open(sys.argv[1], newline="")))
ops = sorted({r["op"] for r in rows}, key=["SET", "GET"].index)
servers = [s for s in ("redis", "redis-rs") if any(r["server"] == s for r in rows)]
meta = rows[0]
versions = {r["server"]: r["version"] for r in rows}


def value(server, op, col):
    return float(next(r[col] for r in rows if r["server"] == server and r["op"] == op))


fig, axes = plt.subplots(1, 2, figsize=(11, 4.6), facecolor=SURFACE)
panels = [
    ("ops_per_sec", "Throughput (requests / s) - higher is better", lambda v: f"{v / 1000:.1f}k"),
    ("p99_ms", "p99 latency (ms) - lower is better", lambda v: f"{v:.2f}"),
]
width = 0.36
for ax, (col, title, fmt) in zip(axes, panels):
    ax.set_facecolor(SURFACE)
    for i, server in enumerate(servers):
        xs = [j + (i - (len(servers) - 1) / 2) * (width + 0.03) for j in range(len(ops))]
        ys = [value(server, op, col) for op in ops]
        bars = ax.bar(xs, ys, width, color=COLORS[server], label=f"{server} {versions[server]}", zorder=3)
        for b, y in zip(bars, ys):
            ax.annotate(fmt(y), (b.get_x() + b.get_width() / 2, y), xytext=(0, 3),
                        textcoords="offset points", ha="center", va="bottom", fontsize=9, color=INK)
    ax.set_xticks(range(len(ops)))
    ax.set_xticklabels(ops, color=INK2)
    ax.set_title(title, loc="left", fontsize=10.5, color=INK, pad=10)
    ax.grid(axis="y", color=GRID, linewidth=0.8, zorder=0)
    ax.set_axisbelow(True)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.tick_params(axis="y", colors=INK2, length=0)
    ax.tick_params(axis="x", length=0)
    ax.set_ylim(0, ax.get_ylim()[1] * 1.12)
    if col == "ops_per_sec":
        ax.set_yticks(ax.get_yticks())
        ax.set_yticklabels([f"{t / 1000:.0f}k" for t in ax.get_yticks()])

handles, labels = axes[0].get_legend_handles_labels()
fig.legend(handles, labels, frameon=False, loc="upper right", bbox_to_anchor=(0.995, 0.995), ncol=2,
           fontsize=9.5, labelcolor=INK2)

fig.suptitle(
    f"redis-rs vs redis - {meta['clients']} clients, {int(meta['n_ops']):,} requests per run, median of runs",
    x=0.01, ha="left", fontsize=12.5, color=INK, y=0.99,
)
note = (
    f"{meta['host_cpu']} | {meta['os']} | official redis-benchmark, same network namespace. "
    "redis is single-threaded; redis-rs runs a multi-threaded runtime; the default benchmark uses ONE key "
    "(one shard); redis-benchmark itself is single-threaded, so both servers may be client-bound. "
    "See README for caveats."
)
fig.text(0.01, 0.012, textwrap.fill(note, 175), fontsize=7.4, color=INK2, ha="left", va="bottom")
fig.tight_layout(rect=(0, 0.11, 1, 0.94))
fig.savefig(sys.argv[2], dpi=160, facecolor=SURFACE)
print(f"wrote {sys.argv[2]}")
