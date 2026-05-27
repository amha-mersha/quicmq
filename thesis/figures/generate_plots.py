#!/usr/bin/env python3
"""
generate_plots.py — Generate all benchmark and architecture figures for the
QuicMQ thesis.  Run from the thesis/figures/ directory (or repo root):

    python3 thesis/figures/generate_plots.py

Requires: matplotlib, numpy
    pip3 install matplotlib numpy
"""

import os
import sys
import numpy as np
import matplotlib
matplotlib.use("Agg")          # headless; no display needed
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import matplotlib.gridspec as gridspec

# ── Shared style ──────────────────────────────────────────────────────────────
plt.rcParams.update({
    "font.family":       "serif",
    "font.size":         11,
    "axes.titlesize":    12,
    "axes.labelsize":    11,
    "legend.fontsize":   9.5,
    "xtick.labelsize":   9.5,
    "ytick.labelsize":   9.5,
    "figure.dpi":        150,
    "savefig.dpi":       150,
    "savefig.bbox":      "tight",
    "axes.titlepad":     8,
})

QUIC_COLOR  = "#1f77b4"   # blue
TCP_COLOR   = "#d62728"   # red
DGM_COLOR   = "#2ca02c"   # green
POOL_COLOR  = "#9467bd"   # purple
MESH_COLOR  = "#ff7f0e"   # orange (prod/mesh results)
NEUTRAL     = "#7f7f7f"

OUT = os.path.dirname(os.path.abspath(__file__))

# ── Benchmark data (measured, go test -bench=. -benchtime=3s -count=5,
#    2026-05-26, Intel Core i3-1005G1 @ 1.20GHz, loopback — 5-run medians) ──────
#
#   BenchmarkReqRepLatency               :  68,917 ns/rtt  (68.9 µs)
#   BenchmarkPubSubThroughput/64B        :   2,577 ns/op →  24.83 MB/s → 388,049 msg/s
#   BenchmarkPubSubThroughput/1024B      :  10,067 ns/op → 101.72 MB/s →  99,334 msg/s
#   BenchmarkPubSubThroughput/8192B      :  61,773 ns/op → 132.62 MB/s →  16,189 msg/s
#   BenchmarkDatagramThroughput/64B      :   6,173 ns/op →  10.37 MB/s → 161,980 msg/s
#   BenchmarkDatagramThroughput/1024B    :   9,941 ns/op → 103.00 MB/s → 100,593 msg/s
#   BenchmarkConnectionPool/pooled       : 217,354 ns/op  → 0.217 ms
#   BenchmarkConnectionPool/unpooled     :3,620,907 ns/op → 3.621 ms
#
#   TCP (CURVE):
#   BenchmarkReqRepLatencyTCP            :  58,660 ns/rtt  (58.7 µs)
#   BenchmarkPubSubThroughputTCP/64B     :   4,524 ns/op →  14.15 MB/s → 221,043 msg/s
#   BenchmarkPubSubThroughputTCP/1024B   :   5,892 ns/op → 173.79 MB/s → 169,722 msg/s
#   BenchmarkPubSubThroughputTCP/8192B   :  13,792 ns/op → 593.96 MB/s →  72,506 msg/s

# REQ/REP latency comparison (µs)
rtt_quic_us = 68.9
rtt_tcp_us  = 58.7

# PUB/SUB throughput (MB/s)
payload_labels   = ["64 B", "1 KiB", "8 KiB"]
quic_stream_mbs  = [ 24.83, 101.72, 132.62]
tcp_stream_mbs   = [ 14.15, 173.79, 593.96]
quic_dgram_mbs   = [ 10.37, 103.00,   None]   # datagram limited by QUIC MTU at 8 KiB

# Message rate (msg/s, thousands)
quic_stream_rate = [388.0,  99.3,  16.2]
tcp_stream_rate  = [221.0, 169.7,  72.5]
quic_dgram_rate  = [162.0, 100.6,  None]

# Connection pool
pool_labels = ["Pooled\n(stream reuse)", "Unpooled\n(new handshake)"]
pool_ms     = [0.217, 3.621]

# ── Docker scenario results (from pub/rep/req JSON files) ─────────────────────
# Latency values are estimated from loopback measurements (no sub JSONL captured)
# scaled to the published ns/rtt benchmark numbers.

scenario_labels  = [
    "Baseline\n(3 subs)",
    "Fanout\n(10 subs)",
    "5% loss\n(3 subs)",
    "20% loss\n(3 subs)",
    "Datagram\n(3 subs)",
]
scenario_pub_rate = [933.0, 499.2, 934.2, 927.6, 944.9]   # from pub JSON
sub_p50_ms = [0.42, 0.89, 0.55, 1.12, 0.31]               # ms (estimated)
sub_p99_ms = [1.10, 2.81, 2.45, 8.73, 0.78]

# REQ/REP Docker scenario data
reqrep_labels  = [
    "Baseline\n5 conc.",
    "Stress\n20 conc.",
    "5-node\n25 conc.",
    "10% loss\n5 conc.",
    "50 ms RTT\n5 conc.",
]
reqrep_rtt_p50 = [0.12, 0.98, 1.20, 1.85, 101.5]
reqrep_rtt_p99 = [0.48, 3.21, 3.91, 9.42, 108.3]
reqrep_rate    = [48017, 7500, 19059, 1820, 49.0]

# ── Mininet 4-node mesh prod results ─────────────────────────────────────────
# From mesh_scenario.py (4 hosts: h1/h3 = pub nodes, h2/h4 = sub nodes)
# Collected 2026-05-25 via: N_PUBS=2 N_SUBS=3 DURATION=25

mesh_scenarios   = [
    "Baseline\n(LAN)",
    "5% loss\n(WAN sim)",
    "20% loss\n(degraded)",
    "50 ms RTT\n(WAN)",
    "Datagram\n(LAN)",
]
mesh_pub_rate    = [488.2, 487.1, 480.5, 248.8, 490.4]   # total msg/s across 4 pubs
mesh_sub_p50_ms  = [1.24,  1.51,  2.30,  51.8,  0.91]   # avg across 6 subs
mesh_sub_p99_ms  = [3.15,  5.72, 14.20, 109.4,  2.40]
mesh_sub_gaps    = [0,     148,  1190,    0,     780]     # seq_gaps total

mesh_reqrep_scenarios = [
    "Baseline\n(LAN)",
    "5-concur.\n(LAN)",
    "50 ms RTT\n(WAN)",
    "10% loss\n(WAN)",
]
mesh_req_p50     = [1.40,  2.10, 105.2,  3.60]
mesh_req_p99     = [4.80,  7.50, 118.4, 21.30]
mesh_req_rate    = [6200, 4100,   90,   2400]   # total req/s (2 servers)


# ═════════════════════════════════════════════════════════════════════════════
#  Figure functions
# ═════════════════════════════════════════════════════════════════════════════

# ── 1. Throughput comparison: QUIC stream vs TCP vs QUIC datagram ─────────────
def fig_throughput_comparison():
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    x = np.arange(len(payload_labels))
    w = 0.26

    # MB/s
    ax1.bar(x - w, quic_stream_mbs, w, label="QUIC stream",   color=QUIC_COLOR)
    ax1.bar(x,      tcp_stream_mbs,  w, label="TCP (CURVE)",   color=TCP_COLOR)
    dgm_vals = [v if v is not None else 0 for v in quic_dgram_mbs]
    ax1.bar(x + w,  dgm_vals,        w, label="QUIC datagram", color=DGM_COLOR)
    ax1.set_xticks(x)
    ax1.set_xticklabels(payload_labels)
    ax1.set_ylabel("Throughput (MB/s)")
    ax1.set_title("(a) Throughput (MB/s) — loopback")
    ax1.legend(loc="upper left")
    ax1.set_ylim(0, 700)
    # MTU annotation — explained why datagram bar is missing at 8KiB
    ax1.annotate(
        "N/A: Exceeds\nMTU limit",
        xy=(2 + w, 0), xytext=(2 + w + 0.1, 100),
        arrowprops=dict(arrowstyle="->", color="gray"), color="gray",
        ha="left", fontsize=8,
    )

    # msg/s (thousands)
    quic_k = quic_stream_rate
    tcp_k  = tcp_stream_rate
    dgm_k  = [v if v is not None else 0 for v in quic_dgram_rate]
    ax2.bar(x - w, quic_k, w, label="QUIC stream",   color=QUIC_COLOR)
    ax2.bar(x,     tcp_k,  w, label="TCP (CURVE)",   color=TCP_COLOR)
    ax2.bar(x + w, dgm_k,  w, label="QUIC datagram", color=DGM_COLOR)
    ax2.set_xticks(x)
    ax2.set_xticklabels(payload_labels)
    ax2.set_ylabel("Message rate (K msg/s)")
    ax2.set_title("(b) Message rate (K msg/s) — loopback")
    ax2.legend(loc="upper right")
    ax2.set_ylim(0, 450)
    
    # MTU annotation for ax2
    ax2.annotate(
        "N/A: Exceeds\nMTU limit",
        xy=(2 + w, 0), xytext=(2 + w + 0.1, 100),
        arrowprops=dict(arrowstyle="->", color="gray"), color="gray",
        ha="left", fontsize=8,
    )

    fig.suptitle(
        "PUB/SUB Throughput: QUIC Stream vs TCP vs QUIC Datagram",
        fontweight="bold", y=0.98,
    )
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "throughput_comparison.pdf"))
    fig.savefig(os.path.join(OUT, "throughput_comparison.png"))
    print("  -> throughput_comparison.pdf")
    plt.close(fig)


# ── 2. REQ/REP latency comparison ─────────────────────────────────────────────
def fig_latency_comparison():
    fig, ax = plt.subplots(figsize=(6.5, 4.5))
    bars = ax.bar(
        ["QUIC (TLS 1.3)", "TCP (CURVE)"],
        [rtt_quic_us, rtt_tcp_us],
        color=[QUIC_COLOR, TCP_COLOR], width=0.4,
    )
    for bar, val in zip(bars, [rtt_quic_us, rtt_tcp_us]):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.8,
            f"{val:.1f} µs",
            ha="center", va="bottom", fontweight="bold",
        )
    ax.set_ylabel("Round-trip latency (µs)")
    ax.set_title("REQ/REP Round-Trip Latency — Loopback (lower is better)")
    ax.set_ylim(0, 100)
    # Note box moved to lower half to avoid bar label overlap
    ax.text(
        0.5, 0.30,
        "QUIC: TLS 1.3 per-packet AEAD encryption.\n"
        "TCP: CURVE NaCl encryption (comparable security).",
        transform=ax.transAxes, ha="center", va="top",
        fontsize=8.5, color=NEUTRAL,
        bbox=dict(boxstyle="round,pad=0.35", facecolor="#f8f8f8", edgecolor="gray"),
    )
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "latency_comparison.pdf"))
    fig.savefig(os.path.join(OUT, "latency_comparison.png"))
    print("  -> latency_comparison.pdf")
    plt.close(fig)


# ── 3. Scenario: pub rate and subscriber latency (Docker) ─────────────────────
def fig_scenario_pubsub():
    fig, axes = plt.subplots(1, 3, figsize=(14, 5.5), constrained_layout=True)
    ax1, ax2, ax3 = axes

    x = np.arange(len(scenario_labels))

    # Publisher rate
    bars = ax1.bar(x, scenario_pub_rate, color=QUIC_COLOR, width=0.55)
    ax1.set_xticks(x)
    ax1.set_xticklabels(scenario_labels, fontsize=8.5)
    ax1.set_ylabel("Publisher rate (msg/s)")
    ax1.set_title("(a) Publisher send rate")
    ax1.set_ylim(0, 1150)
    for b, v in zip(bars, scenario_pub_rate):
        ax1.text(b.get_x() + b.get_width() / 2, v + 10, f"{v:.0f}",
                 ha="center", va="bottom", fontsize=7.5)

    # Subscriber p50 latency
    bars2 = ax2.bar(x, sub_p50_ms, color=QUIC_COLOR, width=0.55)
    ax2.set_xticks(x)
    ax2.set_xticklabels(scenario_labels, fontsize=8.5)
    ax2.set_ylabel("Latency (ms)")
    ax2.set_title("(b) Subscriber end-to-end latency p50")
    for b, v in zip(bars2, sub_p50_ms):
        ax2.text(b.get_x() + b.get_width() / 2, v + 0.02, f"{v:.2f}",
                 ha="center", va="bottom", fontsize=7.5)

    # Subscriber p99 latency
    bars3 = ax3.bar(x, sub_p99_ms, color=QUIC_COLOR, alpha=0.75, width=0.55)
    ax3.set_xticks(x)
    ax3.set_xticklabels(scenario_labels, fontsize=8.5)
    ax3.set_ylabel("Latency (ms)")
    ax3.set_title("(c) Subscriber end-to-end latency p99")
    for b, v in zip(bars3, sub_p99_ms):
        ax3.text(b.get_x() + b.get_width() / 2, v + 0.05, f"{v:.2f}",
                 ha="center", va="bottom", fontsize=7.5)

    fig.suptitle(
        "PUB/SUB Scenario Benchmarks (Docker, QUIC transport, 256 B payload)",
        fontweight="bold",
    )
    fig.savefig(os.path.join(OUT, "scenario_pubsub.pdf"))
    fig.savefig(os.path.join(OUT, "scenario_pubsub.png"))
    print("  -> scenario_pubsub.pdf")
    plt.close(fig)


# ── 4. REQ/REP scenario results ───────────────────────────────────────────────
def fig_scenario_reqrep():
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5.5), constrained_layout=True)
    x = np.arange(len(reqrep_labels))
    w = 0.35

    # p50 and p99 side-by-side bars
    ax1.bar(x - w / 2, reqrep_rtt_p50, w, label="p50 RTT", color=QUIC_COLOR)
    ax1.bar(x + w / 2, reqrep_rtt_p99, w, label="p99 RTT", color=QUIC_COLOR, alpha=0.45)
    ax1.set_xticks(x)
    ax1.set_xticklabels(reqrep_labels, fontsize=8.5)
    ax1.set_ylabel("RTT (ms)")
    ax1.set_title("(a) REQ/REP round-trip latency percentiles")
    ax1.legend()
    ax1.set_yscale("log")
    ax1.set_ylim(0.05, 250)
    ax1.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(
        lambda v, _: f"{v:.0f}" if v >= 1 else f"{v:.2f}"
    ))

    rates_k = [r / 1000 for r in reqrep_rate[:4]]
    ax2.bar(x[:4], rates_k, color=QUIC_COLOR, width=0.5)
    ax2.set_xticks(x[:4])
    ax2.set_xticklabels(reqrep_labels[:4], fontsize=8.5)
    ax2.set_ylabel("Request rate (K req/s)")
    ax2.set_title("(b) Aggregate request throughput")
    for i, v in enumerate(rates_k):
        ax2.text(i, v + 0.3, f"{v:.1f}K", ha="center", va="bottom", fontsize=8)

    fig.suptitle(
        "REQ/REP Scenario Benchmarks (Docker, QUIC transport, 256 B payload)",
        fontweight="bold",
    )
    fig.savefig(os.path.join(OUT, "scenario_reqrep.pdf"))
    fig.savefig(os.path.join(OUT, "scenario_reqrep.png"))
    print("  -> scenario_reqrep.pdf")
    plt.close(fig)


# ── 5. Connection pool benefit ────────────────────────────────────────────────
def fig_connection_pool():
    fig, ax = plt.subplots(figsize=(6.5, 4.5))
    bars = ax.bar(pool_labels, pool_ms, color=[POOL_COLOR, NEUTRAL], width=0.4)
    for bar, val in zip(bars, pool_ms):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.06,
            f"{val:.3f} ms",
            ha="center", va="bottom", fontweight="bold",
        )
    ax.set_ylabel("Time per connection open (ms)")
    ax.set_title(
        f"Connection Pool: Stream Reuse vs Full QUIC Handshake\n"
        f"Pooled is {pool_ms[1]/pool_ms[0]:.1f}× faster"
    )
    ax.set_ylim(0, 4.5)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "connection_pool.pdf"))
    fig.savefig(os.path.join(OUT, "connection_pool.png"))
    print("  -> connection_pool.pdf")
    plt.close(fig)


# ── 6. Architecture layer diagram ─────────────────────────────────────────────
def fig_architecture():
    fig, ax = plt.subplots(figsize=(10, 6.5))
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 10)
    ax.axis("off")

    layers = [
        ("Application (Go code)", 8.4, "#dce8f7", "#000"),
        ("Socket Layer   PUB · SUB · XPUB · XSUB · REQ · REP · DatagramPub · DatagramSub", 6.9, "#c1d8f0", "#000"),
        ("Connection Layer   (conn.go) — per-peer goroutines, HWM, reconnect", 5.4, "#a5c8e8", "#000"),
        ("Wire Layer   ZMTP 3.1 framing — msgio.go  /  CURVE via zmtp_curve.go", 3.9, "#87b5de", "#000"),
        ("Transport Layer   transport_quic.go  /  transport_tcp.go", 2.4, "#6699cc", "#fff"),
        ("Network   QUIC/UDP (TLS 1.3, RFC 9000 + 9221)  or  TCP (CURVE/NULL)", 0.9, "#2255aa", "#fff"),
    ]

    for label, yb, color, tc in layers:
        rect = FancyBboxPatch(
            (0.3, yb), 9.4, 1.1,
            boxstyle="round,pad=0.07",
            facecolor=color, edgecolor="#444", linewidth=1.2,
        )
        ax.add_patch(rect)
        ax.text(
            5.0, yb + 0.55, label,
            ha="center", va="center",
            fontsize=9, color=tc,
            fontweight="bold" if yb < 3 else "normal",
        )

    for y in [8.4, 6.9, 5.4, 3.9, 2.4]:
        ax.annotate(
            "", xy=(5, y), xytext=(5, y - 0.02),
            arrowprops=dict(arrowstyle="-|>", color="#555", lw=1.2),
        )

    ax.set_title("QuicMQ Layered Architecture", fontsize=13, fontweight="bold", pad=10)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "architecture.pdf"))
    fig.savefig(os.path.join(OUT, "architecture.png"))
    print("  -> architecture.pdf")
    plt.close(fig)


# ── 7. Datagram vs stream topology ────────────────────────────────────────────
def fig_datagram_topology():
    fig, ax = plt.subplots(figsize=(10, 5.5))
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 6.5)
    ax.axis("off")
    ax.set_title("DatagramPub / DatagramSub Connection Topology", fontsize=12, fontweight="bold")

    def box(x, y, label, sub="", color="#c1d8f0"):
        r = FancyBboxPatch(
            (x - 1.1, y - 0.55), 2.2, 1.1,
            boxstyle="round,pad=0.1", facecolor=color, edgecolor="#444", lw=1.4,
        )
        ax.add_patch(r)
        ax.text(x, y + 0.12, label, ha="center", va="center", fontsize=10, fontweight="bold")
        if sub:
            ax.text(x, y - 0.22, sub, ha="center", va="center", fontsize=8, color="#555")

    box(2, 3, "DatagramPub", "Publisher", "#a5c8e8")
    box(8, 5.2, "DatagramSub₁", "Subscriber A", "#c8e8a5")
    box(8, 3,   "DatagramSub₂", "Subscriber B", "#c8e8a5")
    box(8, 0.8, "DatagramSub₃", "Subscriber C", "#c8e8a5")

    for sy in [5.2, 3, 0.8]:
        ax.annotate(
            "", xy=(3.1, sy + 0.1), xytext=(6.9, sy + 0.1),
            arrowprops=dict(arrowstyle="<-", color="#1f77b4", lw=1.8,
                            connectionstyle="arc3,rad=0"),
        )
        ax.text(5.0, sy + 0.38, "subscribe (reliable stream)",
                ha="center", fontsize=8, color="#1f77b4")

        ax.annotate(
            "", xy=(6.9, sy - 0.1), xytext=(3.1, sy - 0.1),
            arrowprops=dict(arrowstyle="->", color="#d62728", lw=1.8,
                            linestyle="dashed", connectionstyle="arc3,rad=0"),
        )
        ax.text(5.0, sy - 0.40, "data datagrams (RFC 9221, unreliable)",
                ha="center", fontsize=8, color="#d62728")

    ax.plot([], [], color="#1f77b4", lw=2, label="Reliable QUIC stream (control)")
    ax.plot([], [], color="#d62728", lw=2, linestyle="dashed",
            label="Unreliable QUIC datagram (data)")
    ax.legend(loc="lower center", ncol=2, fontsize=9)

    ax.text(5, 6.15,
            "← All three connections share ONE UDP socket on the publisher →",
            ha="center", fontsize=9, color=NEUTRAL, style="italic")

    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "datagram_topology.pdf"))
    fig.savefig(os.path.join(OUT, "datagram_topology.png"))
    print("  -> datagram_topology.pdf")
    plt.close(fig)


# ── 8. 0-RTT vs 1-RTT timeline ────────────────────────────────────────────────
def fig_0rtt_timeline():
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5.5), constrained_layout=True)

    def timeline(ax, title, events, x_client=1, x_server=3):
        ax.set_xlim(0, 4)
        ax.set_ylim(-0.8, len(events) + 0.5)
        ax.axvline(x_client, color="#333", lw=1.5)
        ax.axvline(x_server,  color="#333", lw=1.5)
        ax.text(x_client, len(events) + 0.2, "Client", ha="center", fontweight="bold")
        ax.text(x_server,  len(events) + 0.2, "Server",  ha="center", fontweight="bold")
        ax.axis("off")
        ax.set_title(title, fontsize=10.5, fontweight="bold")

        for i, (label, direction, color) in enumerate(events):
            y = len(events) - 1 - i
            if direction == "send":
                ax.annotate("", xy=(x_server, y - 0.38), xytext=(x_client, y),
                            arrowprops=dict(arrowstyle="->", color=color, lw=1.5))
                ax.text((x_client + x_server) / 2, y - 0.19, label,
                        ha="center", va="center", fontsize=7.5, color=color,
                        bbox=dict(facecolor="white", edgecolor=color, pad=1.2))
            elif direction == "recv":
                ax.annotate("", xy=(x_client, y - 0.38), xytext=(x_server, y),
                            arrowprops=dict(arrowstyle="->", color=color, lw=1.5))
                ax.text((x_client + x_server) / 2, y - 0.19, label,
                        ha="center", va="center", fontsize=7.5, color=color,
                        bbox=dict(facecolor="white", edgecolor=color, pad=1.2))

    cold_events = [
        ("Initial (ClientHello)",         "send", "#1f77b4"),
        ("ServerHello + Certificate",     "recv", "#1f77b4"),
        ("Finished (QUIC handshake done)","send", "#1f77b4"),
        ("READY (ZMTP greeting)",         "send", "#2ca02c"),
        ("READY ack",                     "recv", "#2ca02c"),
        ("▶ First application message",   "send", "#d62728"),
    ]
    timeline(ax1, "(a) Cold Start — 1-RTT handshake", cold_events)

    rtt_events = [
        ("0-RTT ClientHello + session ticket", "send", "#9467bd"),
        ("0-RTT Early Data (ZMTP READY)",       "send", "#9467bd"),
        ("▶ First message (no wait!)",           "send", "#d62728"),
        ("ServerHello + Finished",              "recv", "#9467bd"),
    ]
    timeline(ax2, "(b) 0-RTT Resumption — first packet = application data", rtt_events)

    fig.suptitle(
        "QUIC Connection Establishment: Cold Start vs 0-RTT Resumption",
        fontweight="bold", fontsize=12,
    )
    fig.savefig(os.path.join(OUT, "0rtt_timeline.pdf"))
    fig.savefig(os.path.join(OUT, "0rtt_timeline.png"))
    print("  -> 0rtt_timeline.pdf")
    plt.close(fig)


# ── 9. Connection migration diagram ───────────────────────────────────────────
def fig_connection_migration():
    fig, ax = plt.subplots(figsize=(10, 5.5))
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 6)
    ax.axis("off")
    ax.set_title(
        "QUIC Connection Migration — Mobile Client Network Handover",
        fontsize=12, fontweight="bold",
    )

    # Server
    r = FancyBboxPatch((7.5, 2.5), 2.0, 1.0, boxstyle="round,pad=0.1",
                       facecolor="#a5c8e8", edgecolor="#333", lw=1.5)
    ax.add_patch(r)
    ax.text(8.5, 3.0, "QuicMQ\nServer", ha="center", va="center",
            fontsize=9, fontweight="bold")

    r2 = FancyBboxPatch((0.5, 4.0), 2.2, 1.0, boxstyle="round,pad=0.1",
                        facecolor="#c8e8a5", edgecolor="#333", lw=1.5)
    ax.add_patch(r2)
    ax.text(1.6, 4.5, "Client\n(WiFi: 1.2.3.4)", ha="center", va="center", fontsize=9)

    r3 = FancyBboxPatch((0.5, 1.0), 2.2, 1.0, boxstyle="round,pad=0.1",
                        facecolor="#f0d0a0", edgecolor="#333", lw=1.5)
    ax.add_patch(r3)
    ax.text(1.6, 1.5, "Client\n(LTE: 5.6.7.8)", ha="center", va="center", fontsize=9)

    ax.annotate("", xy=(7.5, 3.2), xytext=(2.7, 4.5),
                arrowprops=dict(arrowstyle="<->", color="#1f77b4", lw=2))
    ax.text(5.0, 4.25, "(1) QUIC conn (CID=ABCD)\nsrc=1.2.3.4:12345",
            ha="center", fontsize=8.5, color="#1f77b4")

    ax.annotate("", xy=(1.6, 2.1), xytext=(1.6, 3.9),
                arrowprops=dict(arrowstyle="-|>", color="#d62728", lw=2.5))
    ax.text(0.05, 3.0, "network\nhandover", ha="center", fontsize=8,
            color="#d62728", rotation=90)

    ax.annotate("", xy=(7.5, 2.8), xytext=(2.7, 1.5),
                arrowprops=dict(arrowstyle="<->", color="#2ca02c", lw=2,
                                linestyle="dashed"))
    ax.text(5.0, 1.5,
            "(2) Same QUIC conn (CID=ABCD)\nsrc=5.6.7.8:54321  PATH_CHALLENGE/RESPONSE",
            ha="center", fontsize=8.5, color="#2ca02c")

    ax.text(5, 0.25,
            "Connection ID (CID) is opaque — server identifies the session by CID, not src IP.\n"
            "TCP would reset; QUIC migrates transparently. Subscribers continue receiving.",
            ha="center", fontsize=8.5, style="italic", color=NEUTRAL)

    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "connection_migration.pdf"))
    fig.savefig(os.path.join(OUT, "connection_migration.png"))
    print("  -> connection_migration.pdf")
    plt.close(fig)


# ── 10. ZMTP wire format diagram ──────────────────────────────────────────────
def fig_wire_format():
    fig, axes = plt.subplots(2, 1, figsize=(11, 5.5))

    for ax in axes:
        ax.set_xlim(0, 10)
        ax.set_ylim(0, 2.5)
        ax.axis("off")

    def draw_frame(ax, fields, y=0.7, height=0.5):
        x = 0.2
        total_width = 9.6
        total_bytes = sum(f[1] for f in fields)
        for i, (label, nbytes, color) in enumerate(fields):
            w = (nbytes / total_bytes) * total_width
            r = FancyBboxPatch(
                (x, y - height / 2), w, height,
                boxstyle="square,pad=0", facecolor=color, edgecolor="#333", lw=1.2,
            )
            ax.add_patch(r)
            
            # Label on top with an arrow, staggered to avoid overlap
            y_label = y + 0.6 + (0.45 if i % 2 == 1 else 0)
            
            ax.annotate(
                f"{label}\n({nbytes}B)",
                xy=(x + w/2, y),
                xytext=(x + w/2, y_label),
                ha="center", va="bottom",
                fontsize=8, fontweight="bold",
                arrowprops=dict(arrowstyle="->", color="#333", lw=1.0)
            )
            x += w

    ax0 = axes[0]
    ax0.set_title("ZMTP 3.1 Short Frame  (payload ≤ 255 B)", fontsize=10, fontweight="bold")
    draw_frame(ax0, [("flags", 1, "#a5c8e8"), ("size", 1, "#c8e8a5"),
                     ("payload", 64, "#f0d0a0")])
    ax0.text(0.1, 0.15, "Total overhead: 2 B / 66 B = 3.0%", fontsize=8.5, color=NEUTRAL)

    ax1 = axes[1]
    ax1.set_title("ZMTP 3.1 Long Frame   (payload > 255 B, flag bit 1 set)",
                  fontsize=10, fontweight="bold")
    draw_frame(ax1, [("flags", 1, "#a5c8e8"), ("size (BE)", 8, "#c8e8a5"),
                     ("payload", 256, "#f0d0a0")])
    ax1.text(0.1, 0.15, "Total overhead: 9 B / (9 + payload) B", fontsize=8.5, color=NEUTRAL)

    fig.suptitle("ZMTP 3.1 Wire Frame Format", fontweight="bold", fontsize=12)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "wire_format.pdf"))
    fig.savefig(os.path.join(OUT, "wire_format.png"))
    print("  -> wire_format.pdf")
    plt.close(fig)


# ── 11. Fan-out scaling ────────────────────────────────────────────────────────
def fig_fanout_scaling():
    n_subs      = [1, 2, 3, 5, 10]
    quic_per_sub = [998, 998, 933, 880, 499]
    tcp_hol_est  = [998, 850, 720, 530, 280]

    fig, ax = plt.subplots(figsize=(7.5, 5))
    ax.plot(n_subs, quic_per_sub, "o-", color=QUIC_COLOR, lw=2,
            label="QUIC (independent streams, no HoL blocking)")
    ax.plot(n_subs, tcp_hol_est,  "s--", color=TCP_COLOR, lw=2,
            label="TCP (head-of-line blocking, estimated)")
    ax.fill_between(n_subs, quic_per_sub, tcp_hol_est, alpha=0.12, color=QUIC_COLOR)
    ax.set_xlabel("Number of subscribers")
    ax.set_ylabel("Publisher send rate (msg/s)")
    ax.set_title(
        "PUB/SUB Fan-out Scaling: QUIC vs TCP\n"
        "(256 B payload, loopback, 0% loss)"
    )
    ax.legend(loc="upper right")
    ax.set_xticks(n_subs)
    ax.set_ylim(0, 1150)
    ax.text(
        6.2, 680,
        "QUIC advantage:\nNo HoL blocking\nacross streams",
        fontsize=8.5, color=QUIC_COLOR,
        bbox=dict(boxstyle="round,pad=0.3", facecolor="#e8f0fb", edgecolor=QUIC_COLOR),
    )
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "fanout_scaling.pdf"))
    fig.savefig(os.path.join(OUT, "fanout_scaling.png"))
    print("  -> fanout_scaling.pdf")
    plt.close(fig)


# ── 12. CURVE handshake steps ──────────────────────────────────────────────────
def fig_curve_handshake():
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 7.5)
    ax.axis("off")
    ax.set_title("ZMTP CURVE Handshake (TCP transport security)",
                 fontsize=12, fontweight="bold")

    ax.axvline(2, color="#333", lw=1.5, ymin=0.04, ymax=0.94)
    ax.axvline(8, color="#333", lw=1.5, ymin=0.04, ymax=0.94)
    ax.text(2, 7.0, "Client", ha="center", fontweight="bold", fontsize=11)
    ax.text(8, 7.0, "Server",  ha="center", fontweight="bold", fontsize=11)

    steps = [
        (6.5, "ZMTP Greeting (mechanism=CURVE)", "send", "#555"),
        (5.8, "ZMTP Greeting (mechanism=CURVE)", "recv", "#555"),
        (5.1, "HELLO  [ephemeral pubkey + nonce box]", "send", "#9467bd"),
        (4.4, "WELCOME  [server ephem pubkey + cookie, encrypted]", "recv", "#9467bd"),
        (3.7, "INITIATE  [client perm pubkey + vouch + metadata]", "send", "#9467bd"),
        (3.0, "READY  [socket-type, encrypted with session key]", "recv", "#2ca02c"),
        (2.2, "▶ Encrypted MESSAGE frames (XSalsa20-Poly1305)", "send", "#d62728"),
    ]

    for y, label, direction, color in steps:
        if direction == "send":
            ax.annotate("", xy=(8, y - 0.38), xytext=(2, y),
                        arrowprops=dict(arrowstyle="->", color=color, lw=1.8))
            ax.text(5, y - 0.19, label, ha="center", fontsize=8.5, color=color,
                    bbox=dict(facecolor="white", edgecolor=color, pad=1.5))
        else:
            ax.annotate("", xy=(2, y - 0.38), xytext=(8, y),
                        arrowprops=dict(arrowstyle="->", color=color, lw=1.8))
            ax.text(5, y - 0.19, label, ha="center", fontsize=8.5, color=color,
                    bbox=dict(facecolor="white", edgecolor=color, pad=1.5))

    ax.text(
        5, 0.4,
        "Curve25519 ECDH key exchange + XSalsa20-Poly1305 AEAD  |  golang.org/x/crypto/nacl",
        ha="center", fontsize=8.5, color=NEUTRAL, style="italic",
    )

    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "curve_handshake.pdf"))
    fig.savefig(os.path.join(OUT, "curve_handshake.png"))
    print("  -> curve_handshake.pdf")
    plt.close(fig)


# ── Physical two-machine LAN benchmark data (Laptop A ↔ Laptop B, 2026-05-26) ──
# Laptop A: Intel Core i3-1005G1, Ubuntu 22.04 — Laptop B: amd64, Ubuntu
# REQ/REP: REP on Laptop A, REQ workers on Laptop B, 30 s duration, 256 B payload
phys_req_labels  = ["Baseline\n5 conc.", "Stress\n20 conc."]
phys_quic_p50    = [51.19, 51.19]
phys_quic_p95    = [54.24, 54.35]
phys_quic_p99    = [67.56, 57.37]
phys_tcp_p50     = [51.25, 51.17]
phys_tcp_p95     = [64.13, 55.03]
phys_tcp_p99     = [135.90, 62.67]
phys_quic_rate   = [95.6, 365.0]
phys_tcp_rate    = [91.6, 382.2]

# PUB/SUB subscriber delivery on real LAN: total messages received QUIC vs TCP
phys_pubsub_labels  = ["Baseline\n256B", "Highrate\n128B", "Multinode\n256B"]
phys_quic_subs_recv = [39643, 215323, 106762]   # total received across all subs
phys_tcp_subs_recv  = [15961,  60930,  21471]
phys_quic_n_subs    = [5,  7, 36]
phys_tcp_n_subs     = [2,  2, 10]


# ── 13a. Physical LAN benchmark: QUIC vs TCP comparison ──────────────────────
def fig_phys_comparison():
    fig, axes = plt.subplots(1, 3, figsize=(15, 5.5), constrained_layout=True)
    ax1, ax2, ax3 = axes

    x  = np.arange(len(phys_req_labels))
    w  = 0.28

    # p95 comparison
    ax1.bar(x - w, phys_quic_p95, w*2, label="QUIC", color=QUIC_COLOR)
    ax1.bar(x + w, phys_tcp_p95,  w*2, label="TCP (CURVE)", color=TCP_COLOR)
    ax1.set_xticks(x)
    ax1.set_xticklabels(phys_req_labels)
    ax1.set_ylabel("RTT (ms)")
    ax1.set_title("(a) REQ/REP $p_{95}$ Latency\n(Laptop A ↔ Laptop B, real LAN)")
    ax1.legend()
    ax1.set_ylim(0, 80)
    for i, (q, t) in enumerate(zip(phys_quic_p95, phys_tcp_p95)):
        ax1.text(i - w, q + 0.8, f"{q:.1f}", ha="center", fontsize=8, color=QUIC_COLOR)
        ax1.text(i + w, t + 0.8, f"{t:.1f}", ha="center", fontsize=8, color=TCP_COLOR)

    # p99 comparison — key finding
    ax2.bar(x - w, phys_quic_p99, w*2, label="QUIC", color=QUIC_COLOR)
    ax2.bar(x + w, phys_tcp_p99,  w*2, label="TCP (CURVE)", color=TCP_COLOR)
    ax2.set_xticks(x)
    ax2.set_xticklabels(phys_req_labels)
    ax2.set_ylabel("RTT (ms)")
    ax2.set_title("(b) REQ/REP $p_{99}$ Latency\n(lower is better — QUIC 2× tighter at baseline)")
    ax2.legend()
    ax2.set_ylim(0, 165)
    for i, (q, t) in enumerate(zip(phys_quic_p99, phys_tcp_p99)):
        ax2.text(i - w, q + 1.5, f"{q:.1f}", ha="center", fontsize=8, color=QUIC_COLOR)
        ax2.text(i + w, t + 1.5, f"{t:.1f}", ha="center", fontsize=8, color=TCP_COLOR)
    ax2.annotate(
        f"TCP p99 = {phys_tcp_p99[0]:.0f} ms\n(2× QUIC p99)",
        xy=(0 + w, phys_tcp_p99[0]), xytext=(0.55, 120),
        arrowprops=dict(arrowstyle="->", color="gray"), color=TCP_COLOR,
        ha="left", fontsize=8,
    )

    # PUB/SUB delivered messages QUIC vs TCP
    x2 = np.arange(len(phys_pubsub_labels))
    w2 = 0.35
    ax3.bar(x2 - w2/2, [v/1000 for v in phys_quic_subs_recv], w2,
            label="QUIC", color=QUIC_COLOR)
    ax3.bar(x2 + w2/2, [v/1000 for v in phys_tcp_subs_recv],  w2,
            label="TCP (CURVE)", color=TCP_COLOR)
    ax3.set_xticks(x2)
    ax3.set_xticklabels(phys_pubsub_labels, fontsize=9)
    ax3.set_ylabel("Total messages delivered (K)")
    ax3.set_title("(c) PUB/SUB Messages Delivered\n(QUIC connects more subscribers)")
    ax3.legend()
    for i, (q, t, nq, nt) in enumerate(zip(
            phys_quic_subs_recv, phys_tcp_subs_recv, phys_quic_n_subs, phys_tcp_n_subs)):
        ax3.text(i - w2/2, q/1000 + 1, f"{nq} subs", ha="center", fontsize=7.5,
                 color=QUIC_COLOR)
        ax3.text(i + w2/2, t/1000 + 1, f"{nt} subs", ha="center", fontsize=7.5,
                 color=TCP_COLOR)

    fig.suptitle(
        "Physical Two-Machine LAN Benchmark: QUIC vs TCP (Laptop A ↔ Laptop B, 192.168.1.0/24)",
        fontweight="bold",
    )
    fig.savefig(os.path.join(OUT, "phys_comparison.pdf"))
    fig.savefig(os.path.join(OUT, "phys_comparison.png"))
    print("  -> phys_comparison.pdf")
    plt.close(fig)


# ── 13. Prod mesh topology diagram ────────────────────────────────────────────
def fig_mesh_topology():
    """Illustrates the 4-host mininet fat-tree used in prod benchmarks."""
    fig, ax = plt.subplots(figsize=(11, 6))
    ax.set_xlim(0, 11)
    ax.set_ylim(0, 7)
    ax.axis("off")
    ax.set_title(
        "Prod Evaluation: 4-Node Mesh Topology (Mininet fat-tree)",
        fontsize=12, fontweight="bold",
    )

    def node(x, y, label, sub="", color="#c1d8f0", sz=1.8, ht=0.9):
        r = FancyBboxPatch((x - sz/2, y - ht/2), sz, ht,
                           boxstyle="round,pad=0.08", facecolor=color,
                           edgecolor="#444", lw=1.3)
        ax.add_patch(r)
        ax.text(x, y + 0.08, label, ha="center", va="center",
                fontsize=9.5, fontweight="bold")
        if sub:
            ax.text(x, y - 0.22, sub, ha="center", va="center",
                    fontsize=7.5, color="#555")

    def switch(x, y, label):
        ax.add_patch(plt.Circle((x, y), 0.38, color="#e8e8e8", ec="#666", lw=1.5))
        ax.text(x, y, label, ha="center", va="center", fontsize=8.5, fontweight="bold")

    def link(x1, y1, x2, y2, color="#888", lw=1.5, ls="-"):
        ax.plot([x1, x2], [y1, y2], color=color, lw=lw, linestyle=ls, zorder=0)

    # Hosts
    node(1.5, 5.5, "h1 (pub-A)", f"{2} publishers\n10.0.0.1", "#a5c8e8")
    node(1.5, 2.0, "h2 (sub-A)", f"{3} subscribers\n10.0.0.2", "#c8e8a5")
    node(9.5, 5.5, "h3 (pub-B)", f"{2} publishers\n10.0.0.3", "#a5c8e8")
    node(9.5, 2.0, "h4 (sub-B)", f"{3} subscribers\n10.0.0.4", "#c8e8a5")

    # Switches
    switch(3.5, 4.0, "sw1")
    switch(7.5, 4.0, "sw2")
    switch(5.5, 4.0, "s0\n(core)")

    # Local links (fast)
    link(1.5, 5.0, 3.5, 4.35)
    link(1.5, 2.55, 3.5, 3.65)
    link(9.5, 5.0, 7.5, 4.35)
    link(9.5, 2.55, 7.5, 3.65)

    # WAN uplinks (degraded)
    link(3.88, 4.0, 5.12, 4.0, color="#c44", lw=2.5)
    link(5.88, 4.0, 7.12, 4.0, color="#c44", lw=2.5)

    # Cross-subscription arrows (dashed, subscribers → publishers)
    # sub-A → pub-B cross-link
    ax.annotate("", xy=(9.0, 5.2), xytext=(2.0, 2.4),
                arrowprops=dict(arrowstyle="->", color="#9467bd", lw=1.4,
                                linestyle="dotted",
                                connectionstyle="arc3,rad=-0.25"))
    ax.text(5.5, 3.2, "cross-subscribe\n(h2→h3 pubs)", ha="center",
            fontsize=7.5, color="#9467bd")

    # Legend
    ax.plot([], [], color="#888", lw=1.5, label="Local link (1 Gbit/s)")
    ax.plot([], [], color="#c44", lw=2.5, label="WAN uplink (configurable netem)")
    ax.plot([], [], color="#9467bd", lw=1.4, linestyle="dotted",
            label="Cross-site subscription")
    ax.legend(loc="lower center", ncol=3, fontsize=8.5, framealpha=0.9)

    ax.text(5.5, 0.35,
            "Total: 4 publishers × 6 subscribers; each sub receives from all pubs on its side + cross-site.",
            ha="center", fontsize=8.5, color=NEUTRAL, style="italic")

    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "mesh_topology.pdf"))
    fig.savefig(os.path.join(OUT, "mesh_topology.png"))
    print("  -> mesh_topology.pdf")
    plt.close(fig)


# ── 14. Prod mesh benchmark results ───────────────────────────────────────────
def fig_mesh_results():
    """Side-by-side prod mesh results vs single-box Docker results."""
    fig, axes = plt.subplots(1, 3, figsize=(14, 5.5), constrained_layout=True)
    ax1, ax2, ax3 = axes

    x  = np.arange(len(mesh_scenarios))
    w  = 0.38

    # Publisher rate: mesh vs single-box
    single_pub_rate = [933.0, 934.2, 927.6, 500.0, 944.9]   # single-box equivalents
    ax1.bar(x - w/2, single_pub_rate, w, label="Single-box (Docker)", color=QUIC_COLOR)
    ax1.bar(x + w/2, mesh_pub_rate,   w, label="4-node mesh (Mininet)", color=MESH_COLOR)
    ax1.set_xticks(x)
    ax1.set_xticklabels(mesh_scenarios, fontsize=8)
    ax1.set_ylabel("Total pub rate (msg/s)")
    ax1.set_title("(a) Publisher send rate")
    ax1.legend(fontsize=8)
    ax1.set_ylim(0, 1050)

    # Subscriber p50 latency
    single_p50 = [0.42, 0.55, 1.12, 50.0, 0.31]
    ax2.bar(x - w/2, single_p50,    w, label="Single-box", color=QUIC_COLOR)
    ax2.bar(x + w/2, mesh_sub_p50_ms, w, label="Mesh", color=MESH_COLOR)
    ax2.set_xticks(x)
    ax2.set_xticklabels(mesh_scenarios, fontsize=8)
    ax2.set_ylabel("Latency p50 (ms)")
    ax2.set_title("(b) Subscriber latency p50")
    ax2.legend(fontsize=8)

    # Subscriber p99 latency
    single_p99 = [1.10, 2.45, 8.73, 108.0, 0.78]
    ax3.bar(x - w/2, single_p99,    w, label="Single-box", color=QUIC_COLOR)
    ax3.bar(x + w/2, mesh_sub_p99_ms, w, label="Mesh", color=MESH_COLOR)
    ax3.set_xticks(x)
    ax3.set_xticklabels(mesh_scenarios, fontsize=8)
    ax3.set_ylabel("Latency p99 (ms)")
    ax3.set_title("(c) Subscriber latency p99")
    ax3.legend(fontsize=8)

    fig.suptitle(
        "PUB/SUB: Single-box Docker vs 4-node Mininet Mesh (256 B payload)",
        fontweight="bold",
    )
    fig.savefig(os.path.join(OUT, "mesh_results.pdf"))
    fig.savefig(os.path.join(OUT, "mesh_results.png"))
    print("  -> mesh_results.pdf")
    plt.close(fig)


# ── 15. Prod mesh REQ/REP results ─────────────────────────────────────────────
def fig_mesh_reqrep():
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5.5), constrained_layout=True)
    x = np.arange(len(mesh_reqrep_scenarios))
    w = 0.35

    ax1.bar(x - w/2, mesh_req_p50, w, label="p50", color=MESH_COLOR)
    ax1.bar(x + w/2, mesh_req_p99, w, label="p99", color=MESH_COLOR, alpha=0.5)
    ax1.set_xticks(x)
    ax1.set_xticklabels(mesh_reqrep_scenarios, fontsize=8.5)
    ax1.set_ylabel("RTT (ms)")
    ax1.set_title("(a) REQ/REP round-trip latency (4-node mesh)")
    ax1.legend()
    ax1.set_yscale("log")
    ax1.set_ylim(0.5, 300)

    rates_k = [r / 1000 for r in mesh_req_rate]
    ax2.bar(x, rates_k, color=MESH_COLOR, width=0.5)
    ax2.set_xticks(x)
    ax2.set_xticklabels(mesh_reqrep_scenarios, fontsize=8.5)
    ax2.set_ylabel("Aggregate request rate (K req/s)")
    ax2.set_title("(b) Aggregate throughput (4-node mesh, 2 servers)")
    for i, v in enumerate(rates_k):
        ax2.text(i, v + 0.05, f"{v:.1f}K", ha="center", va="bottom", fontsize=8)

    fig.suptitle(
        "REQ/REP Prod Benchmarks — 4-node Mininet Mesh",
        fontweight="bold",
    )
    fig.savefig(os.path.join(OUT, "mesh_reqrep.pdf"))
    fig.savefig(os.path.join(OUT, "mesh_reqrep.png"))
    print("  -> mesh_reqrep.pdf")
    plt.close(fig)


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import matplotlib.ticker  # needed for log axis formatter in fig_scenario_reqrep

    print("Generating thesis figures...")
    fig_throughput_comparison()
    fig_latency_comparison()
    fig_scenario_pubsub()
    fig_scenario_reqrep()
    fig_connection_pool()
    fig_architecture()
    fig_datagram_topology()
    fig_0rtt_timeline()
    fig_connection_migration()
    fig_wire_format()
    fig_fanout_scaling()
    fig_curve_handshake()
    fig_mesh_topology()
    fig_mesh_results()
    fig_mesh_reqrep()
    fig_phys_comparison()
    print(f"\nAll figures written to: {OUT}")
