#!/usr/bin/env bash
# concurrent-reqrep/run.sh — Start a REP server and N client processes each
# running W concurrent workers, collect JSON results, and print aggregated stats.
#
# Usage:
#   ./run.sh [options]
#
# Options:
#   --clients  N    number of client processes     (default 3)
#   --workers  N    goroutines per client process  (default 5)
#   --count    N    requests per worker (-1=∞)    (default 100)
#   --size     N    request payload bytes          (default 64)
#   --dur      D    client run duration            (default 30s)
#   --out      DIR  output directory               (default ./results/<ts>)
#   --help          print this help
#
# Requirements: go, jq
set -euo pipefail

N_CLIENTS=3; N_WORKERS=5; COUNT=100; SIZE=64; DUR=30s; OUT_DIR=""
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --clients) N_CLIENTS="$2"; shift 2 ;;
        --workers) N_WORKERS="$2"; shift 2 ;;
        --count)   COUNT="$2";     shift 2 ;;
        --size)    SIZE="$2";      shift 2 ;;
        --dur)     DUR="$2";       shift 2 ;;
        --out)     OUT_DIR="$2";   shift 2 ;;
        --help|-h)
            grep '^#' "$0" | sed 's/^# \{0,1\}//' | head -18
            exit 0 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

[[ -z "$OUT_DIR" ]] && OUT_DIR="$SCRIPT_DIR/results/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUT_DIR"

PIDS=()
cleanup() {
    for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
    wait "${PIDS[@]:-}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "══════════════════════════════════════════════"
echo "  Concurrent REQ/REP multi-node example"
echo "  clients=$N_CLIENTS  workers/client=$N_WORKERS  count=$COUNT  size=${SIZE}B  dur=$DUR"
echo "  results → $OUT_DIR"
echo "══════════════════════════════════════════════"

# Total duration = client dur + buffer for server
SERVER_DUR="${DUR%s}"; SERVER_DUR=$(( SERVER_DUR + 15 ))

# ── Server ────────────────────────────────────────────────────────────────────
SRV_OUT="$OUT_DIR/server.json"
go run "$SCRIPT_DIR/server" \
    -addr "quic://127.0.0.1:9400" \
    -dur  "${SERVER_DUR}s" \
    -output "$SRV_OUT" \
    -id "server" &
PIDS+=($!)
echo "Started server (pid ${PIDS[-1]})"
sleep 1  # wait for bind

# ── Clients ───────────────────────────────────────────────────────────────────
for i in $(seq 1 "$N_CLIENTS"); do
    CLI_OUT="$OUT_DIR/client-${i}.json"
    go run "$SCRIPT_DIR/client" \
        -addr    "quic://127.0.0.1:9400" \
        -workers "$N_WORKERS" \
        -count   "$COUNT" \
        -dur     "$DUR" \
        -size    "$SIZE" \
        -output  "$CLI_OUT" \
        -id      "client-${i}" &
    PIDS+=($!)
    echo "Started client-${i} (pid ${PIDS[-1]})"
done

echo ""
echo "Running for $DUR…"
# Wait for all client processes.
for i in $(seq 2 $((N_CLIENTS + 1))); do
    wait "${PIDS[$((i-1))]}" 2>/dev/null || true
done
sleep 2
kill "${PIDS[0]}" 2>/dev/null || true  # stop server

# ── Aggregation ───────────────────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════"
echo "  Results summary  (transport=quic)"
echo "══════════════════════════════════════════════"
command -v jq >/dev/null 2>&1 || { echo "(install jq for aggregation)"; exit 0; }

[[ -f "$SRV_OUT" ]] && jq -r \
    '"  server  handled=\(.reqs_handled)  rate=\(.actual_rate | floor) req/s  err=\(.errors)"' \
    "$SRV_OUT" 2>/dev/null || true

CLI_FILES=("$OUT_DIR"/client-*.json)
for f in "${CLI_FILES[@]}"; do
    [[ -f "$f" ]] || continue
    jq -r '"  \(.id | .[0:8])  sent=\(.reqs_sent)  rate=\(.actual_rate | floor) req/s  avg=\(.rtt_avg_ms // 0 | . * 100 | floor | . / 100)ms  p50=\(.rtt_p50_ms // 0 | . * 100 | floor | . / 100)ms  p99=\(.rtt_p99_ms // 0 | . * 100 | floor | . / 100)ms  err=\(.errors)"' \
        "$f" 2>/dev/null || true
done

TOTAL_SENT=$(jq -s '[.[].reqs_sent] | add' "${CLI_FILES[@]}" 2>/dev/null || echo 0)
TOTAL_ERRS=$(jq -s '[.[].errors] | add' "${CLI_FILES[@]}" 2>/dev/null || echo 0)
AVG_P50=$(jq -s '[.[].rtt_p50_ms // 0] | add / length' "${CLI_FILES[@]}" 2>/dev/null || echo 0)
AVG_P99=$(jq -s '[.[].rtt_p99_ms // 0] | add / length' "${CLI_FILES[@]}" 2>/dev/null || echo 0)
echo ""
echo "  Aggregate: total_sent=$TOTAL_SENT  errors=$TOTAL_ERRS  avg_p50=${AVG_P50}ms  avg_p99=${AVG_P99}ms"
echo "══════════════════════════════════════════════"
echo "  Full results in $OUT_DIR"
