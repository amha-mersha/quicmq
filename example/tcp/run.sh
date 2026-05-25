#!/usr/bin/env bash
# example/tcp/run.sh — Start a TCP+CURVE publisher and N subscribers in parallel,
# wait for the run to finish, and aggregate the JSON results.
#
# Usage:
#   ./run.sh [options]
#
# Options:
#   --subs   N    number of subscriber nodes  (default 3)
#   --rate   N    publisher messages/second   (default 1000)
#   --size   N    payload bytes per message   (default 256)
#   --dur    D    run duration, e.g. 30s      (default 30s)
#   --topic  T    topic prefix                (default news)
#   --out    DIR  directory for JSON results  (default ./results/<timestamp>)
#   --help        print this help
#
# Requirements:  go, jq
set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
N_SUBS=3
RATE=1000
SIZE=256
DUR=30s
TOPIC=news
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR=""

# ── Arg parsing ───────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --subs)  N_SUBS="$2";  shift 2 ;;
        --rate)  RATE="$2";    shift 2 ;;
        --size)  SIZE="$2";    shift 2 ;;
        --dur)   DUR="$2";     shift 2 ;;
        --topic) TOPIC="$2";   shift 2 ;;
        --out)   OUT_DIR="$2"; shift 2 ;;
        --help|-h)
            grep '^#' "$0" | sed 's/^# \{0,1\}//' | head -20
            exit 0 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

if [[ -z "$OUT_DIR" ]]; then
    OUT_DIR="$SCRIPT_DIR/results/$(date +%Y%m%d_%H%M%S)"
fi
mkdir -p "$OUT_DIR"

KEY_FILE="$OUT_DIR/server_pk.hex"
PIDS=()

cleanup() {
    for pid in "${PIDS[@]:-}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait "${PIDS[@]:-}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "══════════════════════════════════════════════"
echo "  TCP+CURVE multi-node example"
echo "  subs=$N_SUBS  rate=$RATE/s  size=${SIZE}B  dur=$DUR  topic=$TOPIC"
echo "  results → $OUT_DIR"
echo "══════════════════════════════════════════════"

# ── Publisher ─────────────────────────────────────────────────────────────────
PUB_OUT="$OUT_DIR/pub.json"
go run "$SCRIPT_DIR/publisher" \
    -addr "tcp://127.0.0.1:9200" \
    -topic "$TOPIC" \
    -rate "$RATE" \
    -size "$SIZE" \
    -dur  "$DUR" \
    -key-file "$KEY_FILE" \
    -output "$PUB_OUT" \
    -id "pub" &
PIDS+=($!)
echo "Started publisher (pid ${PIDS[-1]})"

# Wait for key file to appear.
echo -n "Waiting for CURVE key file..."
for _ in $(seq 1 40); do
    [[ -f "$KEY_FILE" ]] && break
    sleep 0.25
done
if [[ ! -f "$KEY_FILE" ]]; then
    echo " TIMEOUT — publisher did not write key file" >&2
    exit 1
fi
echo " done ($(cat "$KEY_FILE"))"
sleep 1  # let the publisher bind fully

# ── Subscribers ───────────────────────────────────────────────────────────────
for i in $(seq 1 "$N_SUBS"); do
    SUB_OUT="$OUT_DIR/sub-${i}.json"
    go run "$SCRIPT_DIR/subscriber" \
        -addr     "tcp://127.0.0.1:9200" \
        -topic    "$TOPIC" \
        -key-file "$KEY_FILE" \
        -dur      "$DUR" \
        -output   "$SUB_OUT" \
        -id       "sub-${i}" &
    PIDS+=($!)
    echo "Started subscriber-${i} (pid ${PIDS[-1]})"
done

echo ""
echo "Running for $DUR — press Ctrl-C to stop early…"
wait "${PIDS[0]}" 2>/dev/null || true  # wait for publisher to finish
sleep 2  # allow subscribers to flush their output

# ── Aggregation ───────────────────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════"
echo "  Results summary"
echo "══════════════════════════════════════════════"
command -v jq >/dev/null 2>&1 || { echo "(install jq for aggregation)"; exit 0; }

# Publisher stats
if [[ -f "$PUB_OUT" ]]; then
    jq -r '"  pub     sent=\(.msgs_sent)  rate=\(.actual_rate | floor) msg/s  \(.throughput_mbs | . * 100 | floor | . / 100) MB/s  errors=\(.errors)"' \
        "$PUB_OUT" 2>/dev/null || true
fi

# Subscriber stats
SUB_FILES=("$OUT_DIR"/sub-*.json)
if [[ ${#SUB_FILES[@]} -gt 0 ]] && [[ -f "${SUB_FILES[0]}" ]]; then
    for f in "${SUB_FILES[@]}"; do
        [[ -f "$f" ]] || continue
        jq -r '"  \(.id | .[0:8])  rcvd=\(.msgs_received)  gaps=\(.seq_gaps)  p50=\(.latency_p50_ms // 0 | . * 100 | floor | . / 100)ms  p99=\(.latency_p99_ms // 0 | . * 100 | floor | . / 100)ms  rate=\(.actual_rate | floor) msg/s"' \
            "$f" 2>/dev/null || true
    done

    # Average latency across all subscribers
    AVG_P50=$(jq -s '[.[].latency_p50_ms // 0] | add / length' "${SUB_FILES[@]}" 2>/dev/null || echo 0)
    AVG_P99=$(jq -s '[.[].latency_p99_ms // 0] | add / length' "${SUB_FILES[@]}" 2>/dev/null || echo 0)
    TOTAL_GAPS=$(jq -s '[.[].seq_gaps] | add' "${SUB_FILES[@]}" 2>/dev/null || echo 0)
    echo ""
    echo "  Aggregate: avg_p50=${AVG_P50}ms  avg_p99=${AVG_P99}ms  total_gaps=${TOTAL_GAPS}"
fi

echo "══════════════════════════════════════════════"
echo "  Full results in $OUT_DIR"
