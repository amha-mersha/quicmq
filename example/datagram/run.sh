#!/usr/bin/env bash
# example/datagram/run.sh — Start a datagram publisher and N subscribers,
# optionally apply packet loss/delay via tc-netem on the loopback interface,
# wait for the run, and aggregate JSON results.
#
# Usage:
#   ./run.sh [options]
#
# Options:
#   --subs    N    subscriber node count           (default 3)
#   --rate    N    publisher messages/second       (default 1000)
#   --size    N    payload bytes (max ~1200)       (default 256)
#   --dur     D    run duration, e.g. 30s          (default 30s)
#   --topic   T    topic prefix                   (default sensor)
#   --loss    N    packet loss % via netem (0=off) (default 0)
#   --delay   N    one-way delay ms via netem     (default 0)
#   --out     DIR  output directory               (default ./results/<ts>)
#   --help         print this help
#
# Requirements:  go, jq
# Netem options: requires root and iproute2 (tc command)
set -euo pipefail

N_SUBS=3; RATE=1000; SIZE=256; DUR=30s; TOPIC=sensor
LOSS=0; DELAY=0; OUT_DIR=""
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --subs)   N_SUBS="$2";  shift 2 ;;
        --rate)   RATE="$2";    shift 2 ;;
        --size)   SIZE="$2";    shift 2 ;;
        --dur)    DUR="$2";     shift 2 ;;
        --topic)  TOPIC="$2";   shift 2 ;;
        --loss)   LOSS="$2";    shift 2 ;;
        --delay)  DELAY="$2";   shift 2 ;;
        --out)    OUT_DIR="$2"; shift 2 ;;
        --help|-h)
            grep '^#' "$0" | sed 's/^# \{0,1\}//' | head -22
            exit 0 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

[[ -z "$OUT_DIR" ]] && OUT_DIR="$SCRIPT_DIR/results/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUT_DIR"

PIDS=()
NETEM_APPLIED=false

cleanup() {
    for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
    wait "${PIDS[@]:-}" 2>/dev/null || true
    if $NETEM_APPLIED; then
        echo "Removing netem rules from lo..."
        sudo tc qdisc del dev lo root 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

echo "══════════════════════════════════════════════"
echo "  Datagram (RFC 9221) multi-node example"
echo "  subs=$N_SUBS  rate=$RATE/s  size=${SIZE}B  dur=$DUR"
echo "  loss=${LOSS}%  delay=${DELAY}ms"
echo "  results → $OUT_DIR"
echo "══════════════════════════════════════════════"

# ── Optional netem on loopback ────────────────────────────────────────────────
if [[ "$LOSS" -gt 0 || "$DELAY" -gt 0 ]]; then
    if ! command -v tc &>/dev/null; then
        echo "WARNING: tc not found — netem simulation skipped" >&2
    else
        NETEM_OPTS=""
        [[ "$DELAY" -gt 0 ]] && NETEM_OPTS="delay ${DELAY}ms"
        [[ "$LOSS" -gt 0 ]]  && NETEM_OPTS="$NETEM_OPTS loss ${LOSS}%"
        echo "Applying netem to lo: $NETEM_OPTS (requires sudo)"
        sudo tc qdisc replace dev lo root netem $NETEM_OPTS
        NETEM_APPLIED=true
    fi
fi

# ── Publisher ─────────────────────────────────────────────────────────────────
PUB_OUT="$OUT_DIR/pub.json"
go run "$SCRIPT_DIR/publisher" \
    -addr "quic://127.0.0.1:9300" -topic "$TOPIC" \
    -rate "$RATE" -size "$SIZE" -dur "$DUR" \
    -output "$PUB_OUT" -id "pub" &
PIDS+=($!)
echo "Started publisher (pid ${PIDS[-1]})"
sleep 1  # let publisher bind

# ── Subscribers ───────────────────────────────────────────────────────────────
for i in $(seq 1 "$N_SUBS"); do
    SUB_OUT="$OUT_DIR/sub-${i}.json"
    go run "$SCRIPT_DIR/subscriber" \
        -addr "quic://127.0.0.1:9300" -topic "$TOPIC" \
        -dur "$DUR" -output "$SUB_OUT" -id "sub-${i}" &
    PIDS+=($!)
    echo "Started subscriber-${i} (pid ${PIDS[-1]})"
done

echo ""
echo "Running for $DUR…"
wait "${PIDS[0]}" 2>/dev/null || true
sleep 2

# ── Aggregation ───────────────────────────────────────────────────════════════
echo ""
echo "══════════════════════════════════════════════"
echo "  Results summary  (transport=quic-datagram)"
echo "══════════════════════════════════════════════"
command -v jq >/dev/null 2>&1 || { echo "(install jq for aggregation)"; exit 0; }

[[ -f "$PUB_OUT" ]] && jq -r \
    '"  pub   sent=\(.msgs_sent)  rate=\(.actual_rate | floor) msg/s  \(.throughput_mbs | . * 100 | floor | . / 100) MB/s"' \
    "$PUB_OUT" 2>/dev/null || true

SUB_FILES=("$OUT_DIR"/sub-*.json)
TOTAL_SENT=$(jq -r '.msgs_sent // 0' "$PUB_OUT" 2>/dev/null || echo 0)

for f in "${SUB_FILES[@]}"; do
    [[ -f "$f" ]] || continue
    jq --arg sent "$TOTAL_SENT" -r \
        '"  \(.id | .[0:8])  rcvd=\(.msgs_received)  gaps=\(.seq_gaps)  loss=\(if ($sent|tonumber) > 0 then (.seq_gaps / ($sent|tonumber) * 100 | . * 10 | floor | . / 10) else 0 end)%  p50=\(.latency_p50_ms // 0 | . * 100 | floor | . / 100)ms  p99=\(.latency_p99_ms // 0 | . * 100 | floor | . / 100)ms"' \
        "$f" 2>/dev/null || true
done

AVG_P50=$(jq -s '[.[].latency_p50_ms // 0] | add / length' "${SUB_FILES[@]}" 2>/dev/null || echo 0)
AVG_P99=$(jq -s '[.[].latency_p99_ms // 0] | add / length' "${SUB_FILES[@]}" 2>/dev/null || echo 0)
TOTAL_GAPS=$(jq -s '[.[].seq_gaps] | add' "${SUB_FILES[@]}" 2>/dev/null || echo 0)
echo ""
echo "  Aggregate: avg_p50=${AVG_P50}ms  avg_p99=${AVG_P99}ms  total_gaps=${TOTAL_GAPS}"
echo "══════════════════════════════════════════════"
echo "  Full results in $OUT_DIR"
