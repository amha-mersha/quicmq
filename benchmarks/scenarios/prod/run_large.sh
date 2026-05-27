#!/usr/bin/env bash
# run_large.sh — Run all large-scale Mininet scenarios for QuicMQ thesis.
#
# Usage:  sudo bash benchmarks/scenarios/prod/run_large.sh
#
# Runs 12 scenarios across PUB/SUB, REQ/REP, and Datagram patterns with
# different network conditions (baseline, loss, latency, bandwidth limit).
# Results are written to benchmarks/scenarios/prod/results/<scenario>/
# and a summary JSON is written to prod/results/summary.json.
#
# Required: mininet (sudo apt install mininet), pre-built binaries in _bin/
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="$SCRIPT_DIR/../_bin"
RESULTS_BASE="$SCRIPT_DIR/results"
SUMMARY="$RESULTS_BASE/summary.json"

# Color output
GREEN='\033[0;32m'; CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'
info()    { printf "${CYAN}[large]${RESET} %s\n" "$*"; }
success() { printf "${GREEN}[OK]${RESET}   %s\n" "$*"; }

# Check that binaries exist
for b in pub sub dpub dsub req rep; do
    [[ -x "$BIN_DIR/$b" ]] || { echo "Missing binary: $BIN_DIR/$b — build first"; exit 1; }
done

run_scenario() {
    local scenario="$1"; shift
    local mode="$1"; shift
    # remaining args are env var assignments: KEY=VALUE …
    local env_vars="$*"

    info "━━━ $scenario ($mode) ━━━"
    local export_str="MODE=$mode SCENARIO=$scenario RESULTS_DIR=$RESULTS_BASE DURATION=20 $env_vars"

    # Run large_scenario.py with sudo (already root if called via sudo)
    env $export_str python3 "$SCRIPT_DIR/large_scenario.py" 2>&1 | sed 's/^/  /'
    success "$scenario done → $RESULTS_BASE/$scenario/"
    echo ""
}

mkdir -p "$RESULTS_BASE"

echo ""
echo -e "${BOLD}╔══════════════════════════════════════════════════════╗"
echo -e "║  QuicMQ Large-Scale Mininet Benchmark Suite          ║"
echo -e "║  $(date '+%Y-%m-%d %H:%M')  — 12 scenarios                  ║"
echo -e "╚══════════════════════════════════════════════════════╝${RESET}"
echo ""

# ── PUB/SUB scenarios (5 pubs → 15 subs = 20 nodes) ──────────────────────────
run_scenario "ps_baseline"       pubsub "N_PUBS=5 N_SUBS=15 MSG_RATE=200 MSG_SIZE=256"
run_scenario "ps_small_payload"  pubsub "N_PUBS=5 N_SUBS=15 MSG_RATE=500 MSG_SIZE=64"
run_scenario "ps_large_payload"  pubsub "N_PUBS=5 N_SUBS=15 MSG_RATE=50  MSG_SIZE=4096"
run_scenario "ps_loss5"          pubsub "N_PUBS=5 N_SUBS=15 MSG_RATE=200 MSG_SIZE=256 NETEM_LOSS_PCT=5"
run_scenario "ps_loss20"         pubsub "N_PUBS=5 N_SUBS=15 MSG_RATE=200 MSG_SIZE=256 NETEM_LOSS_PCT=20"
run_scenario "ps_latency50"      pubsub "N_PUBS=5 N_SUBS=15 MSG_RATE=100 MSG_SIZE=256 NETEM_DELAY_MS=50 NETEM_JITTER_MS=5"

# ── REQ/REP scenarios (3 reps ← 10 reqs = 13 nodes) ─────────────────────────
run_scenario "rr_baseline"       reqrep "N_REPS=3 N_REQS=10 CONCURRENCY=3 MSG_SIZE=256"
run_scenario "rr_highconcur"     reqrep "N_REPS=3 N_REQS=10 CONCURRENCY=10 MSG_SIZE=256"
run_scenario "rr_loss10"         reqrep "N_REPS=3 N_REQS=10 CONCURRENCY=3  MSG_SIZE=256 NETEM_LOSS_PCT=10"
run_scenario "rr_latency50"      reqrep "N_REPS=3 N_REQS=10 CONCURRENCY=3  MSG_SIZE=256 NETEM_DELAY_MS=50"

# ── Datagram scenarios (5 dpubs → 15 dsubs = 20 nodes) ───────────────────────
run_scenario "dgram_baseline"    datagram "N_PUBS=5 N_SUBS=15 MSG_RATE=200 MSG_SIZE=256"
run_scenario "dgram_loss10"      datagram "N_PUBS=5 N_SUBS=15 MSG_RATE=200 MSG_SIZE=256 NETEM_LOSS_PCT=10"

echo -e "${BOLD}All scenarios complete.${RESET}"
echo "Results in: $RESULTS_BASE"
