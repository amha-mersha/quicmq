#!/usr/bin/env bash
# topic-filter/run.sh — Start a publisher and N subscribers with different topic
# subscriptions, collect JSON results, and show per-subscription delivery stats.
#
# Usage:
#   ./run.sh [options]
#
# Options:
#   --subs    N    number of subscriber nodes              (default 3)
#   --topics  T    comma-separated topics for the publisher (default sports,finance,weather)
#   --rate    N    publisher messages/second (total)        (default 300)
#   --size    N    payload bytes                           (default 128)
#   --dur     D    run duration                            (default 30s)
#   --out     DIR  output directory                        (default ./results/<ts>)
#   --help         print this help
#
# Each subscriber is assigned a round-robin subset of the publisher's topics,
# demonstrating that each receives only its matching messages.
#
# Requirements: go, jq
set -euo pipefail

N_SUBS=3; RATE=300; SIZE=128; DUR=30s; OUT_DIR=""
TOPICS="sports,finance,weather"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --subs)   N_SUBS="$2";   shift 2 ;;
        --topics) TOPICS="$2";   shift 2 ;;
        --rate)   RATE="$2";     shift 2 ;;
        --size)   SIZE="$2";     shift 2 ;;
        --dur)    DUR="$2";      shift 2 ;;
        --out)    OUT_DIR="$2";  shift 2 ;;
        --help|-h)
            grep '^#' "$0" | sed 's/^# \{0,1\}//' | head -20
            exit 0 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

[[ -z "$OUT_DIR" ]] && OUT_DIR="$SCRIPT_DIR/results/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUT_DIR"

# Split topics into an array for round-robin assignment.
IFS=',' read -ra TOPIC_ARR <<< "$TOPICS"
N_TOPICS=${#TOPIC_ARR[@]}

PIDS=()
cleanup() {
    for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
    wait "${PIDS[@]:-}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "══════════════════════════════════════════════"
echo "  Topic-filter multi-node example"
echo "  topics=$TOPICS  subs=$N_SUBS  rate=$RATE/s  size=${SIZE}B  dur=$DUR"
echo "  results → $OUT_DIR"
echo "══════════════════════════════════════════════"

# ── Publisher ─────────────────────────────────────────────────────────────────
PUB_OUT="$OUT_DIR/pub.json"
go run "$SCRIPT_DIR/publisher" \
    -addr "quic://127.0.0.1:9500" \
    -topics "$TOPICS" \
    -rate "$RATE" \
    -size "$SIZE" \
    -dur  "$DUR" \
    -output "$PUB_OUT" \
    -id "pub" &
PIDS+=($!)
echo "Started publisher (pid ${PIDS[-1]})"
sleep 1  # let publisher bind

# ── Subscribers (round-robin topic assignment) ────────────────────────────────
for i in $(seq 1 "$N_SUBS"); do
    # Each subscriber gets one topic from the round-robin list.
    topic_idx=$(( (i - 1) % N_TOPICS ))
    SUB_TOPIC="${TOPIC_ARR[$topic_idx]}"
    SUB_OUT="$OUT_DIR/sub-${i}.json"
    go run "$SCRIPT_DIR/subscriber" \
        -addr   "quic://127.0.0.1:9500" \
        -topics "$SUB_TOPIC" \
        -dur    "$DUR" \
        -output "$SUB_OUT" \
        -id     "sub-${i}(${SUB_TOPIC})" &
    PIDS+=($!)
    echo "Started subscriber-${i} → topic='$SUB_TOPIC' (pid ${PIDS[-1]})"
done

echo ""
echo "Running for $DUR…"
wait "${PIDS[0]}" 2>/dev/null || true
sleep 2

# ── Aggregation ───────────────────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════"
echo "  Results summary  (topic filtering)"
echo "══════════════════════════════════════════════"
command -v jq >/dev/null 2>&1 || { echo "(install jq for aggregation)"; exit 0; }

[[ -f "$PUB_OUT" ]] && jq -r \
    '"  pub   sent=\(.msgs_sent)  rate=\(.actual_rate | floor) msg/s  \(.throughput_mbs | . * 100 | floor | . / 100) MB/s"' \
    "$PUB_OUT" 2>/dev/null || true

SUB_FILES=("$OUT_DIR"/sub-*.json)
for f in "${SUB_FILES[@]}"; do
    [[ -f "$f" ]] || continue
    jq -r '"  \(.id | .[0:18])  rcvd=\(.msgs_received)  rate=\(.actual_rate | floor) msg/s"' \
        "$f" 2>/dev/null || true
done

echo ""
echo "  Per-subscriber details:"
for f in "${SUB_FILES[@]}"; do
    [[ -f "$f" ]] || continue
    sid=$(jq -r '.id' "$f")
    jq -r --arg id "$sid" '
        .per_topic // {} | to_entries[] |
        "    [\($id)] topic=\(.key)  rcvd=\(.value.msgs_received)  p50=\(.value.latency_p50_ms // 0 | . * 100 | floor | . / 100)ms  p99=\(.value.latency_p99_ms // 0 | . * 100 | floor | . / 100)ms"
    ' "$f" 2>/dev/null || true
done

echo "══════════════════════════════════════════════"
echo "  Full results in $OUT_DIR"
