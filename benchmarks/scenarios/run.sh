#!/usr/bin/env bash
# run.sh — QuicMQ scenario test runner
#
# Usage:
#   ./run.sh                    # run all scenarios sequentially
#   ./run.sh list               # print available scenario names
#   ./run.sh <name> [<name>…]   # run specific scenarios
#   ./run.sh build              # (re)build the Docker image only
#
# Requirements:
#   docker compose v2, jq
#
# Results are written to ./results/<scenario>/ and a summary table is printed
# at the end.  Each run is reproducible: identical env vars → identical
# container configuration.
#
# Network simulation is applied to the CLIENT-side containers only (sub, req,
# dsub), simulating a mobile client on a degraded network while the server
# remains pristine.  Delay values are one-way; RTT = 2 × NETEM_DELAY_MS.
set -euo pipefail

# ── Paths ────────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
RESULTS_ROOT="$SCRIPT_DIR/results"

# Change to the repository root so the build context is correct.
cd "$SCRIPT_DIR/../.."

# ── Helpers ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

info()    { printf "${CYAN}[run]${RESET} %s\n" "$*"; }
success() { printf "${GREEN}[OK]${RESET}  %s\n" "$*"; }
warn()    { printf "${YELLOW}[!!]${RESET}  %s\n" "$*"; }
die()     { printf "${RED}[ERR]${RESET} %s\n" "$*" >&2; exit 1; }

require() { command -v "$1" >/dev/null 2>&1 || die "Required tool not found: $1"; }

# ── Core runner ───────────────────────────────────────────────────────────────
#
# run_scenario NAME SERVICES SCALE_FLAGS [ENV_OVERRIDES]
#
#   NAME          scenario identifier, used for results directory
#   SERVICES      space-separated list of docker compose service names to start
#   SCALE_FLAGS   extra --scale arguments (e.g. "--scale sub=5")
#
# All SCENARIO_* variables exported before calling this function are passed
# through to the containers as environment variables.
run_scenario() {
    require docker; require jq
    docker compose version >/dev/null 2>&1 || die "docker compose v2 required (not docker-compose v1)"
    local name="$1"; shift
    local services="$1"; shift          # e.g. "pub sub"
    local scale_flags="${1:-}"; shift || true

    local out_dir="$RESULTS_ROOT/$name"
    mkdir -p "$out_dir"

    printf "\n${BOLD}━━━ Scenario: %s ━━━${RESET}\n" "$name"
    info "services: $services  scale: ${scale_flags:-default}"
    info "duration: ${DURATION:-30}s  rate: ${MSG_RATE:-500}/s  size: ${MSG_SIZE:-256}B"
    info "netem: delay=${NETEM_DELAY_MS:-0}ms jitter=${NETEM_JITTER_MS:-0}ms \
loss=${NETEM_LOSS_PCT:-0}% rate=${NETEM_RATE_KBIT:-0}kbit \
corrupt=${NETEM_CORRUPT_PCT:-0}% reorder=${NETEM_REORDER_PCT:-0}%"

    # Clean up previous containers for this project.
    local project="quicmq-${name//_/-}"
    docker compose -f "$COMPOSE_FILE" -p "$project" down --remove-orphans -v 2>/dev/null || true

    # Export scenario name so containers embed it in their JSON output.
    export SCENARIO="$name"

    # Run the scenario.  --abort-on-container-exit stops everything when the
    # first container exits; since all containers run for exactly DURATION
    # seconds and clients exit slightly before the server, this is fine.
    local start_ts
    start_ts=$(date +%s)

    # shellcheck disable=SC2086
    docker compose \
        -f "$COMPOSE_FILE" \
        -p "$project" \
        up \
        --abort-on-container-exit \
        $scale_flags \
        $services \
        2>&1 | sed "s/^/  /" || true

    local end_ts
    end_ts=$(date +%s)
    local wall=$((end_ts - start_ts))

    # Collect JSON results written to the shared volume.
    # Also capture any JSON lines from container stdout via docker logs.
    for svc in $services; do
        docker compose -f "$COMPOSE_FILE" -p "$project" logs --no-color "$svc" 2>/dev/null \
          | grep -E '^\s*\{' \
          | while read -r line; do
              echo "$line" >> "$out_dir/${svc}.jsonl"
            done
    done

    # Tear down.
    docker compose -f "$COMPOSE_FILE" -p "$project" down --remove-orphans -v 2>/dev/null || true

    success "scenario '$name' completed in ${wall}s — results in $out_dir/"
    print_summary "$out_dir"
}

# Print a quick human-readable summary from the JSON files in a results dir.
print_summary() {
    local dir="$1"
    [ -d "$dir" ] || return
    printf "\n  ${BOLD}Results summary:${RESET}\n"
    for f in "$dir"/*.jsonl "$dir"/*.json; do
        [ -f "$f" ] || continue
        # Each file may contain multiple JSON objects (one per container replica).
        while IFS= read -r obj; do
            [ -z "$obj" ] && continue
            role=$(echo "$obj" | jq -r '.role // "?"')
            case "$role" in
            pub|dpub)
                sent=$(echo "$obj"   | jq -r '.msgs_sent   // 0')
                rate=$(echo "$obj"   | jq -r '.actual_rate // 0 | floor')
                mbps=$(echo "$obj"   | jq -r '.throughput_mbs // 0 | . * 100 | floor | . / 100')
                printf "    %-6s  sent=%-8s  rate=%-6s msg/s  %.2f MB/s\n" \
                    "$role" "$sent" "$rate" "$mbps"
                ;;
            sub|dsub)
                rcvd=$(echo "$obj"   | jq -r '.msgs_received  // 0')
                rate=$(echo "$obj"   | jq -r '.actual_rate    // 0 | floor')
                gaps=$(echo "$obj"   | jq -r '.seq_gaps       // 0')
                p50=$(echo "$obj"    | jq -r '.latency_p50_ms // 0 | . * 100 | floor | . / 100')
                p99=$(echo "$obj"    | jq -r '.latency_p99_ms // 0 | . * 100 | floor | . / 100')
                printf "    %-6s  rcvd=%-8s  rate=%-6s msg/s  gaps=%-5s  p50=%.2fms  p99=%.2fms\n" \
                    "$role" "$rcvd" "$rate" "$gaps" "$p50" "$p99"
                ;;
            rep)
                hdl=$(echo "$obj"    | jq -r '.reqs_handled // 0')
                rate=$(echo "$obj"   | jq -r '.actual_rate  // 0 | floor')
                printf "    %-6s  handled=%-8s  rate=%-6s req/s\n" "$role" "$hdl" "$rate"
                ;;
            req)
                sent=$(echo "$obj"   | jq -r '.reqs_sent   // 0')
                rate=$(echo "$obj"   | jq -r '.actual_rate // 0 | floor')
                p50=$(echo "$obj"    | jq -r '.rtt_p50_ms  // 0 | . * 100 | floor | . / 100')
                p99=$(echo "$obj"    | jq -r '.rtt_p99_ms  // 0 | . * 100 | floor | . / 100')
                errs=$(echo "$obj"   | jq -r '.errors      // 0')
                printf "    %-6s  sent=%-8s  rate=%-6s req/s  p50=%.2fms  p99=%.2fms  err=%s\n" \
                    "$role" "$sent" "$rate" "$p50" "$p99" "$errs"
                ;;
            esac
        done < <(jq -c '.' "$f" 2>/dev/null || cat "$f")
    done
    echo
}

# ── Scenario definitions ──────────────────────────────────────────────────────
# Each function sets SCENARIO_* env vars and calls run_scenario.
# Parameters are documented inline.
#
# Network degradation is applied to CLIENT containers only (sub/req/dsub).
# The server container uses default env (no netem).

# Resets all netem vars to baseline (no degradation).
reset_net() {
    export NETEM_DELAY_MS=0 NETEM_JITTER_MS=0 NETEM_LOSS_PCT=0
    export NETEM_RATE_KBIT=0 NETEM_CORRUPT_PCT=0 NETEM_REORDER_PCT=0
}

# ┌─ PUB/SUB scenarios ────────────────────────────────────────────────────────┐

scenario_pubsub_baseline() {
    # Establishes the clean-network baseline.
    # 1 publisher, 3 subscribers, moderate rate.
    reset_net
    export TOPIC=data MSG_RATE=1000 MSG_SIZE=256 DURATION=30
    run_scenario "pubsub_baseline" "pub sub" "--scale sub=3"
}

scenario_pubsub_fanout_stress() {
    # Fan-out to many subscribers — tests QUIC's per-stream flow control
    # independence.  Each subscriber gets its own QUIC stream.
    reset_net
    export TOPIC=data MSG_RATE=500 MSG_SIZE=256 DURATION=30
    run_scenario "pubsub_fanout_stress" "pub sub" "--scale sub=10"
}

scenario_pubsub_highrate() {
    # High message rate — measures raw PUB/SUB throughput ceiling.
    reset_net
    export TOPIC=data MSG_RATE=5000 MSG_SIZE=128 DURATION=30
    run_scenario "pubsub_highrate" "pub sub" "--scale sub=3"
}

scenario_pubsub_largemsg() {
    # Large messages — approaches QUIC stream-level MTU; tests framing overhead.
    reset_net
    export TOPIC=data MSG_RATE=200 MSG_SIZE=8192 DURATION=30
    run_scenario "pubsub_largemsg" "pub sub" "--scale sub=3"
}

scenario_pubsub_loss_5pct() {
    # 5% random packet loss on client side — QUIC recovers transparently for
    # stream-based delivery; latency and throughput should degrade gracefully.
    reset_net
    export TOPIC=data MSG_RATE=1000 MSG_SIZE=256 DURATION=30
    export NETEM_LOSS_PCT=5
    run_scenario "pubsub_loss_5pct" "pub sub" "--scale sub=3"
}

scenario_pubsub_loss_20pct() {
    # 20% loss — severe degradation; shows QUIC retransmission cost.
    reset_net
    export TOPIC=data MSG_RATE=1000 MSG_SIZE=256 DURATION=30
    export NETEM_LOSS_PCT=20
    run_scenario "pubsub_loss_20pct" "pub sub" "--scale sub=3"
}

scenario_pubsub_latency_50ms() {
    # 50ms one-way delay (100ms RTT) — typical inter-continental link.
    # 0-RTT session resumption shines here on reconnects.
    reset_net
    export TOPIC=data MSG_RATE=500 MSG_SIZE=256 DURATION=30
    export NETEM_DELAY_MS=50 NETEM_JITTER_MS=5
    run_scenario "pubsub_latency_50ms" "pub sub" "--scale sub=3"
}

scenario_pubsub_latency_200ms() {
    # 200ms one-way delay (400ms RTT) — satellite/very poor link.
    reset_net
    export TOPIC=data MSG_RATE=200 MSG_SIZE=256 DURATION=30
    export NETEM_DELAY_MS=200 NETEM_JITTER_MS=20
    run_scenario "pubsub_latency_200ms" "pub sub" "--scale sub=2"
}

scenario_pubsub_bandwidth_1mbit() {
    # 1 Mbit/s bandwidth cap — QUIC flow control should regulate sending rate.
    reset_net
    export TOPIC=data MSG_RATE=2000 MSG_SIZE=512 DURATION=30
    export NETEM_RATE_KBIT=1000
    run_scenario "pubsub_bandwidth_1mbit" "pub sub" "--scale sub=2"
}

scenario_pubsub_lossy_latency() {
    # Combined: 5% loss + 50ms delay + 5ms jitter — realistic mobile network.
    reset_net
    export TOPIC=data MSG_RATE=500 MSG_SIZE=256 DURATION=30
    export NETEM_DELAY_MS=50 NETEM_JITTER_MS=5 NETEM_LOSS_PCT=5
    run_scenario "pubsub_lossy_latency" "pub sub" "--scale sub=3"
}

# ┌─ REQ/REP scenarios ─────────────────────────────────────────────────────────┐

scenario_reqrep_baseline() {
    # Clean-network baseline: 5 concurrent clients, echo server.
    reset_net
    export MSG_SIZE=256 DURATION=30 CONCURRENCY=5
    run_scenario "reqrep_baseline" "rep req" "--scale req=1"
}

scenario_reqrep_stress() {
    # Many concurrent clients from a single container.
    reset_net
    export MSG_SIZE=256 DURATION=30 CONCURRENCY=20
    run_scenario "reqrep_stress" "rep req" "--scale req=1"
}

scenario_reqrep_multinode_stress() {
    # Many client containers each with 5 concurrent sockets.
    # Total: 5 containers × 5 goroutines = 25 concurrent REQ sockets.
    reset_net
    export MSG_SIZE=256 DURATION=30 CONCURRENCY=5
    run_scenario "reqrep_multinode_stress" "rep req" "--scale req=5"
}

scenario_reqrep_loss_10pct() {
    # 10% packet loss — QUIC retransmits lost packets; RTT percentiles rise.
    reset_net
    export MSG_SIZE=256 DURATION=30 CONCURRENCY=5
    export NETEM_LOSS_PCT=10
    run_scenario "reqrep_loss_10pct" "rep req" "--scale req=1"
}

scenario_reqrep_latency_50ms() {
    # 50ms one-way delay.  RTT P50 should be ~100ms.
    reset_net
    export MSG_SIZE=256 DURATION=30 CONCURRENCY=5
    export NETEM_DELAY_MS=50 NETEM_JITTER_MS=5
    run_scenario "reqrep_latency_50ms" "rep req" "--scale req=1"
}

scenario_reqrep_latency_100ms() {
    # 100ms one-way delay.  RTT P50 should be ~200ms.
    reset_net
    export MSG_SIZE=256 DURATION=30 CONCURRENCY=5
    export NETEM_DELAY_MS=100 NETEM_JITTER_MS=10
    run_scenario "reqrep_latency_100ms" "rep req" "--scale req=1"
}

scenario_reqrep_lossy_latency() {
    # 10% loss + 50ms delay — challenging combined condition.
    reset_net
    export MSG_SIZE=256 DURATION=30 CONCURRENCY=5
    export NETEM_DELAY_MS=50 NETEM_JITTER_MS=5 NETEM_LOSS_PCT=10
    run_scenario "reqrep_lossy_latency" "rep req" "--scale req=1"
}

scenario_reqrep_reorder() {
    # 10% packet reordering — stresses QUIC's in-order delivery guarantee.
    reset_net
    export MSG_SIZE=256 DURATION=30 CONCURRENCY=5
    export NETEM_REORDER_PCT=10 NETEM_DELAY_MS=10
    run_scenario "reqrep_reorder" "rep req" "--scale req=1"
}

# ┌─ Datagram PUB/SUB scenarios (RFC 9221) ────────────────────────────────────┐

scenario_datagram_baseline() {
    # Clean-network baseline for RFC 9221 unreliable datagrams.
    # Compare with pubsub_baseline to see stream vs. datagram overhead.
    reset_net
    export TOPIC=data MSG_RATE=1000 MSG_SIZE=256 DURATION=30
    run_scenario "datagram_baseline" "dpub dsub" "--scale dsub=3"
}

scenario_datagram_highrate() {
    # High-rate datagram delivery — datagrams skip retransmission so
    # throughput should stay higher under congestion than streams.
    reset_net
    export TOPIC=data MSG_RATE=5000 MSG_SIZE=256 DURATION=30
    run_scenario "datagram_highrate" "dpub dsub" "--scale dsub=3"
}

scenario_datagram_loss_5pct() {
    # 5% loss — datagrams are NOT retransmitted; seq_gaps directly measures
    # application-visible loss, unlike the stream case where QUIC hides losses.
    reset_net
    export TOPIC=data MSG_RATE=1000 MSG_SIZE=256 DURATION=30
    export NETEM_LOSS_PCT=5
    run_scenario "datagram_loss_5pct" "dpub dsub" "--scale dsub=3"
}

scenario_datagram_loss_20pct() {
    # 20% loss — illustrates the reliability trade-off.
    # seq_gaps should be ~20% of msgs_sent; latency_p99 should stay low
    # (no head-of-line blocking from retransmits).
    reset_net
    export TOPIC=data MSG_RATE=1000 MSG_SIZE=256 DURATION=30
    export NETEM_LOSS_PCT=20
    run_scenario "datagram_loss_20pct" "dpub dsub" "--scale dsub=3"
}

scenario_datagram_vs_stream() {
    # Run stream and datagram PUB/SUB side-by-side under identical 5% loss.
    # Both patterns receive messages from their respective publishers.
    # Compare the seq_gaps field: stream should be 0, datagram ~5%.
    reset_net
    export TOPIC=data MSG_RATE=1000 MSG_SIZE=256 DURATION=30
    export NETEM_LOSS_PCT=5
    run_scenario "datagram_vs_stream_stream_side" "pub sub" "--scale sub=2"
    run_scenario "datagram_vs_stream_dgram_side" "dpub dsub" "--scale dsub=2"
}

scenario_datagram_latency() {
    # 50ms one-way delay — datagrams show lower tail latency than streams
    # because there is no HOL blocking from stream retransmit.
    reset_net
    export TOPIC=data MSG_RATE=500 MSG_SIZE=256 DURATION=30
    export NETEM_DELAY_MS=50 NETEM_JITTER_MS=5
    run_scenario "datagram_latency_50ms" "dpub dsub" "--scale dsub=3"
}

# ── Scenario registry ─────────────────────────────────────────────────────────
ALL_SCENARIOS=(
    pubsub_baseline
    pubsub_fanout_stress
    pubsub_highrate
    pubsub_largemsg
    pubsub_loss_5pct
    pubsub_loss_20pct
    pubsub_latency_50ms
    pubsub_latency_200ms
    pubsub_bandwidth_1mbit
    pubsub_lossy_latency
    reqrep_baseline
    reqrep_stress
    reqrep_multinode_stress
    reqrep_loss_10pct
    reqrep_latency_50ms
    reqrep_latency_100ms
    reqrep_lossy_latency
    reqrep_reorder
    datagram_baseline
    datagram_highrate
    datagram_loss_5pct
    datagram_loss_20pct
    datagram_vs_stream
    datagram_latency
)

# ── Help ─────────────────────────────────────────────────────────────────────

print_help() {
    cat <<'EOF'

QuicMQ scenario test runner
============================

USAGE
  ./run.sh [dev|prod|phys] [command] [scenario…]
  ./run.sh --help | -h

MODES
  dev   (default) Docker-based scenarios.  Requires docker compose v2 + jq.
                  Network degradation is applied via tc-netem inside containers.

  prod  Mininet-based multi-node scenarios.  Requires mininet (Python) + sudo.
        Simulates two separate machines connected over a configurable link.

  phys  Physical distributed mode.  Runs across two real machines (laptop + Pi).
        Requires SSH key auth to the remote host and Go on both machines.
        The laptop records all results; binaries are cross-compiled and deployed
        automatically.

DEV COMMANDS
  all                Run every dev scenario sequentially (builds image first).
  build              (Re)build the quicmq-scenarios Docker image only.
  list               Print the list of available dev scenario names.
  <name> [<name>…]   Run one or more specific scenarios by name.

PROD COMMANDS
  all                Run every prod scenario sequentially.
  list               Print the list of available prod scenario names.
  <name> [<name>…]   Run one or more specific prod scenarios by name.

PHYS COMMANDS
  all                Run every phys scenario sequentially.
  list               Print the list of available phys scenario names.
  <name> [<name>…]   Run one or more specific phys scenarios by name.

PHYS REQUIRED ENV VARS
  REMOTE_HOST   IP or hostname of the remote machine (required)
  REMOTE_USER   SSH username on the remote (default: pi)

PHYS OPTIONAL ENV VARS
  REMOTE_DIR    working directory on remote (default: /tmp/quicmq-bench)
  REMOTE_ARCH   Go GOARCH for cross-compile (default: amd64)
  LOCAL_IP      local IP reachable from remote (auto-detected)
  LOCAL_PUBS    publishers on laptop (default: 2)
  LOCAL_SUBS    subscribers on laptop (default: 5)
  REMOTE_PUBS   publishers on remote (default: 2)
  REMOTE_SUBS   subscribers on remote (default: 5)
  TRANSPORT     quic (default) or tcp — selects transport for all sockets

EXAMPLES
  # Dev — baseline pub/sub (Docker)
  ./run.sh dev pubsub_baseline

  # Dev — all scenarios
  ./run.sh dev all

  # Prod — mininet baseline (requires sudo)
  ./run.sh prod prod_pubsub_baseline

  # Phys — baseline across laptop + Pi
  REMOTE_HOST=192.168.1.42 REMOTE_USER=pi ./run.sh phys phys_pubsub_baseline

  # Phys — full multinode thesis scenario
  REMOTE_HOST=192.168.1.42 LOCAL_PUBS=10 LOCAL_SUBS=30 REMOTE_PUBS=10 REMOTE_SUBS=30 \
    ./run.sh phys phys_pubsub_multinode

  # Phys — all scenarios (QUIC + TCP comparison)
  REMOTE_HOST=192.168.1.42 ./run.sh phys all

  # Phys — TCP transport only (for direct comparison with QUIC results above)
  REMOTE_HOST=192.168.1.42 TRANSPORT=tcp ./run.sh phys phys_tcp_pubsub_baseline

  # Backward-compatible (defaults to dev all)
  ./run.sh

RESULTS
  dev  → benchmarks/scenarios/results/<scenario>/
  prod → benchmarks/scenarios/prod/results/<scenario>/
  phys → benchmarks/scenarios/phys/results/<scenario>/

  Each result directory contains one .jsonl file per service role.

METRICS REPORTED
  pub/dpub  msgs_sent, actual_rate (msg/s), throughput_mbs (MB/s)
  sub/dsub  msgs_received, seq_gaps (dropped), latency_p50_ms, latency_p99_ms
  req       reqs_sent, rtt_p50_ms, rtt_p99_ms, errors
  rep       reqs_handled, actual_rate (req/s)

COMPILE THESIS
  After collecting results, regenerate plots and compile the PDF:
    ./compile_thesis.sh

SEE ALSO
  benchmarks/scenarios/USAGE.md — extended documentation
  benchmarks/scenarios/phys/phys_scenario.py — physical distributed runner
  benchmarks/scenarios/prod/mininet_scenario.py — mininet topology

EOF
}

# ── Prod mode ─────────────────────────────────────────────────────────────────
#
# Each prod_run_scenario call invokes the mininet Python script with env vars.
# Results land in prod/results/<scenario>/.

PROD_RESULTS_ROOT="$SCRIPT_DIR/prod/results"
MININET_SCRIPT="$SCRIPT_DIR/prod/mininet_scenario.py"

prod_run_scenario() {
    local name="$1"
    local mode="${2:-pubsub}"   # pubsub | reqrep | datagram

    local out_dir="$PROD_RESULTS_ROOT/$name"
    mkdir -p "$out_dir"

    printf "\n${BOLD}━━━ Prod scenario: %s ━━━${RESET}\n" "$name"
    info "mode: $mode  duration: ${DURATION:-30}s  rate: ${MSG_RATE:-500}/s  size: ${MSG_SIZE:-256}B"
    info "netem: delay=${NETEM_DELAY_MS:-0}ms jitter=${NETEM_JITTER_MS:-0}ms \
loss=${NETEM_LOSS_PCT:-0}% rate=${NETEM_RATE_KBIT:-0}kbit"

    export SCENARIO="$name" MODE="$mode" RESULTS_DIR="$out_dir"

    local start_ts
    start_ts=$(date +%s)

    if ! sudo python3 "$MININET_SCRIPT"; then
        warn "prod scenario '$name' exited with error"
    fi

    local wall=$(( $(date +%s) - start_ts ))
    success "prod scenario '$name' completed in ${wall}s — results in $out_dir/"
}

# Resets network env vars for prod scenarios.
prod_reset_net() {
    export NETEM_DELAY_MS=0 NETEM_JITTER_MS=0 NETEM_LOSS_PCT=0
    export NETEM_RATE_KBIT=0 NETEM_REORDER_PCT=0
}

# ── Prod scenario definitions ─────────────────────────────────────────────────

prod_scenario_pubsub_baseline() {
    prod_reset_net
    export TOPIC=data MSG_RATE=1000 MSG_SIZE=256 DURATION=30 N_PUBS=1 N_SUBS=3
    prod_run_scenario "prod_pubsub_baseline" "pubsub"
}

prod_scenario_pubsub_fanout() {
    prod_reset_net
    export TOPIC=data MSG_RATE=500 MSG_SIZE=256 DURATION=30 N_PUBS=1 N_SUBS=10
    prod_run_scenario "prod_pubsub_fanout" "pubsub"
}

prod_scenario_pubsub_loss_5pct() {
    prod_reset_net
    export TOPIC=data MSG_RATE=1000 MSG_SIZE=256 DURATION=30 N_PUBS=1 N_SUBS=3
    export NETEM_LOSS_PCT=5
    prod_run_scenario "prod_pubsub_loss_5pct" "pubsub"
}

prod_scenario_pubsub_loss_20pct() {
    prod_reset_net
    export TOPIC=data MSG_RATE=1000 MSG_SIZE=256 DURATION=30 N_PUBS=1 N_SUBS=3
    export NETEM_LOSS_PCT=20
    prod_run_scenario "prod_pubsub_loss_20pct" "pubsub"
}

prod_scenario_pubsub_latency_50ms() {
    prod_reset_net
    export TOPIC=data MSG_RATE=500 MSG_SIZE=256 DURATION=30 N_PUBS=1 N_SUBS=3
    export NETEM_DELAY_MS=50 NETEM_JITTER_MS=5
    prod_run_scenario "prod_pubsub_latency_50ms" "pubsub"
}

prod_scenario_pubsub_latency_200ms() {
    prod_reset_net
    export TOPIC=data MSG_RATE=200 MSG_SIZE=256 DURATION=30 N_PUBS=1 N_SUBS=2
    export NETEM_DELAY_MS=200 NETEM_JITTER_MS=20
    prod_run_scenario "prod_pubsub_latency_200ms" "pubsub"
}

prod_scenario_pubsub_bandwidth_1mbit() {
    prod_reset_net
    export TOPIC=data MSG_RATE=2000 MSG_SIZE=512 DURATION=30 N_PUBS=1 N_SUBS=2
    export NETEM_RATE_KBIT=1000
    prod_run_scenario "prod_pubsub_bandwidth_1mbit" "pubsub"
}

prod_scenario_pubsub_multinode() {
    # 10 publishers on h1, 30 subscribers on h2 — mirrors the thesis "prod" setup.
    prod_reset_net
    export TOPIC=data MSG_RATE=500 MSG_SIZE=256 DURATION=30 N_PUBS=10 N_SUBS=30
    prod_run_scenario "prod_pubsub_multinode" "pubsub"
}

prod_scenario_reqrep_baseline() {
    prod_reset_net
    export MSG_SIZE=256 DURATION=30 N_REQS=5
    prod_run_scenario "prod_reqrep_baseline" "reqrep"
}

prod_scenario_reqrep_stress() {
    prod_reset_net
    export MSG_SIZE=256 DURATION=30 N_REQS=25
    prod_run_scenario "prod_reqrep_stress" "reqrep"
}

prod_scenario_reqrep_latency_50ms() {
    prod_reset_net
    export MSG_SIZE=256 DURATION=30 N_REQS=5
    export NETEM_DELAY_MS=50 NETEM_JITTER_MS=5
    prod_run_scenario "prod_reqrep_latency_50ms" "reqrep"
}

prod_scenario_reqrep_loss_10pct() {
    prod_reset_net
    export MSG_SIZE=256 DURATION=30 N_REQS=5
    export NETEM_LOSS_PCT=10
    prod_run_scenario "prod_reqrep_loss_10pct" "reqrep"
}

prod_scenario_datagram_baseline() {
    prod_reset_net
    export TOPIC=data MSG_RATE=1000 MSG_SIZE=256 DURATION=30 N_SUBS=3
    prod_run_scenario "prod_datagram_baseline" "datagram"
}

prod_scenario_datagram_loss_5pct() {
    prod_reset_net
    export TOPIC=data MSG_RATE=1000 MSG_SIZE=256 DURATION=30 N_SUBS=3
    export NETEM_LOSS_PCT=5
    prod_run_scenario "prod_datagram_loss_5pct" "datagram"
}

prod_scenario_datagram_loss_20pct() {
    prod_reset_net
    export TOPIC=data MSG_RATE=1000 MSG_SIZE=256 DURATION=30 N_SUBS=3
    export NETEM_LOSS_PCT=20
    prod_run_scenario "prod_datagram_loss_20pct" "datagram"
}

# ┌─ TCP comparison prod scenarios ────────────────────────────────────────────┐
# Each mirrors a QUIC scenario but sets TRANSPORT=tcp.
# Run both sets to get head-to-head numbers for the thesis.

prod_scenario_tcp_pubsub_baseline() {
    prod_reset_net
    export TOPIC=data MSG_RATE=1000 MSG_SIZE=256 DURATION=30 N_PUBS=1 N_SUBS=3 TRANSPORT=tcp
    prod_run_scenario "prod_tcp_pubsub_baseline" "pubsub"
}

prod_scenario_tcp_pubsub_loss_5pct() {
    prod_reset_net
    export TOPIC=data MSG_RATE=1000 MSG_SIZE=256 DURATION=30 N_PUBS=1 N_SUBS=3 TRANSPORT=tcp
    export NETEM_LOSS_PCT=5
    prod_run_scenario "prod_tcp_pubsub_loss_5pct" "pubsub"
}

prod_scenario_tcp_pubsub_loss_20pct() {
    prod_reset_net
    export TOPIC=data MSG_RATE=1000 MSG_SIZE=256 DURATION=30 N_PUBS=1 N_SUBS=3 TRANSPORT=tcp
    export NETEM_LOSS_PCT=20
    prod_run_scenario "prod_tcp_pubsub_loss_20pct" "pubsub"
}

prod_scenario_tcp_pubsub_latency_50ms() {
    prod_reset_net
    export TOPIC=data MSG_RATE=500 MSG_SIZE=256 DURATION=30 N_PUBS=1 N_SUBS=3 TRANSPORT=tcp
    export NETEM_DELAY_MS=50 NETEM_JITTER_MS=5
    prod_run_scenario "prod_tcp_pubsub_latency_50ms" "pubsub"
}

prod_scenario_tcp_pubsub_latency_200ms() {
    prod_reset_net
    export TOPIC=data MSG_RATE=200 MSG_SIZE=256 DURATION=30 N_PUBS=1 N_SUBS=2 TRANSPORT=tcp
    export NETEM_DELAY_MS=200 NETEM_JITTER_MS=20
    prod_run_scenario "prod_tcp_pubsub_latency_200ms" "pubsub"
}

prod_scenario_tcp_reqrep_baseline() {
    prod_reset_net
    export MSG_SIZE=256 DURATION=30 N_REQS=5 TRANSPORT=tcp
    prod_run_scenario "prod_tcp_reqrep_baseline" "reqrep"
}

prod_scenario_tcp_reqrep_latency_50ms() {
    prod_reset_net
    export MSG_SIZE=256 DURATION=30 N_REQS=5 TRANSPORT=tcp
    export NETEM_DELAY_MS=50 NETEM_JITTER_MS=5
    prod_run_scenario "prod_tcp_reqrep_latency_50ms" "reqrep"
}

prod_scenario_tcp_reqrep_loss_10pct() {
    prod_reset_net
    export MSG_SIZE=256 DURATION=30 N_REQS=5 TRANSPORT=tcp
    export NETEM_LOSS_PCT=10
    prod_run_scenario "prod_tcp_reqrep_loss_10pct" "reqrep"
}

ALL_PROD_SCENARIOS=(
    # ── QUIC ──────────────────────────────────────────────────────────────────
    prod_pubsub_baseline
    prod_pubsub_fanout
    prod_pubsub_loss_5pct
    prod_pubsub_loss_20pct
    prod_pubsub_latency_50ms
    prod_pubsub_latency_200ms
    prod_pubsub_bandwidth_1mbit
    prod_pubsub_multinode
    prod_reqrep_baseline
    prod_reqrep_stress
    prod_reqrep_latency_50ms
    prod_reqrep_loss_10pct
    prod_datagram_baseline
    prod_datagram_loss_5pct
    prod_datagram_loss_20pct
    # ── TCP (comparison) ──────────────────────────────────────────────────────
    prod_tcp_pubsub_baseline
    prod_tcp_pubsub_loss_5pct
    prod_tcp_pubsub_loss_20pct
    prod_tcp_pubsub_latency_50ms
    prod_tcp_pubsub_latency_200ms
    prod_tcp_reqrep_baseline
    prod_tcp_reqrep_latency_50ms
    prod_tcp_reqrep_loss_10pct
)

# ── Phys mode (physical distributed: laptop + Raspberry Pi) ──────────────────
#
# Runs real binaries over the actual network between two physical machines.
# The laptop orchestrates everything; results are recorded here.
# Requires: REMOTE_HOST env var, SSH key auth to the Pi, Go on both machines.

PHYS_RESULTS_ROOT="$SCRIPT_DIR/phys/results"
PHYS_SCRIPT="$SCRIPT_DIR/phys/phys_scenario.py"

phys_run_scenario() {
    local name="$1"
    local mode="${2:-pubsub}"

    local out_dir="$PHYS_RESULTS_ROOT/$name"
    mkdir -p "$out_dir"

    printf "\n${BOLD}━━━ Phys scenario: %s ━━━${RESET}\n" "$name"
    info "mode: $mode  duration: ${DURATION:-30}s  rate: ${MSG_RATE:-500}/s  size: ${MSG_SIZE:-256}B"
    info "local:  ${LOCAL_PUBS:-2} pub(s) + ${LOCAL_SUBS:-5} sub(s)"
    info "remote: ${REMOTE_PUBS:-2} pub(s) + ${REMOTE_SUBS:-5} sub(s)  [${REMOTE_HOST}]"

    export SCENARIO="$name" MODE="$mode" RESULTS_DIR="$out_dir"

    local start_ts
    start_ts=$(date +%s)

    if ! python3 "$PHYS_SCRIPT"; then
        warn "phys scenario '$name' exited with error"
    fi

    local wall=$(( $(date +%s) - start_ts ))
    success "phys scenario '$name' done in ${wall}s — results in $out_dir/"
}

phys_reset() {
    export LOCAL_PUBS=2 LOCAL_SUBS=5 REMOTE_PUBS=2 REMOTE_SUBS=5
}

# ── Phys scenario definitions ──────────────────────────────────────────────────

phys_scenario_pubsub_baseline() {
    phys_reset
    export TOPIC=data MSG_RATE=1000 MSG_SIZE=256 DURATION=30
    phys_run_scenario "phys_pubsub_baseline" "pubsub"
}

phys_scenario_pubsub_highrate() {
    phys_reset
    export TOPIC=data MSG_RATE=5000 MSG_SIZE=128 DURATION=30
    phys_run_scenario "phys_pubsub_highrate" "pubsub"
}

phys_scenario_pubsub_largemsg() {
    phys_reset
    export TOPIC=data MSG_RATE=200 MSG_SIZE=8192 DURATION=30
    phys_run_scenario "phys_pubsub_largemsg" "pubsub"
}

phys_scenario_pubsub_multinode() {
    # Full thesis scenario: 10 pubs + 30 subs on each machine.
    export LOCAL_PUBS=10 LOCAL_SUBS=30 REMOTE_PUBS=10 REMOTE_SUBS=30
    export TOPIC=data MSG_RATE=2000 MSG_SIZE=256 DURATION=30
    phys_run_scenario "phys_pubsub_multinode" "pubsub"
}

phys_scenario_reqrep_baseline() {
    phys_reset
    export MSG_SIZE=256 DURATION=30 CONCURRENCY=5
    phys_run_scenario "phys_reqrep_baseline" "reqrep"
}

phys_scenario_reqrep_stress() {
    phys_reset
    export MSG_SIZE=256 DURATION=30 CONCURRENCY=20
    phys_run_scenario "phys_reqrep_stress" "reqrep"
}

phys_scenario_datagram_baseline() {
    phys_reset
    export TOPIC=data MSG_RATE=1000 MSG_SIZE=256 DURATION=30
    phys_run_scenario "phys_datagram_baseline" "datagram"
}

phys_scenario_datagram_highrate() {
    phys_reset
    export TOPIC=data MSG_RATE=5000 MSG_SIZE=256 DURATION=30
    phys_run_scenario "phys_datagram_highrate" "datagram"
}

# ┌─ TCP comparison phys scenarios ────────────────────────────────────────────┐

phys_scenario_tcp_pubsub_baseline() {
    phys_reset
    export TOPIC=data MSG_RATE=1000 MSG_SIZE=256 DURATION=30 TRANSPORT=tcp
    phys_run_scenario "phys_tcp_pubsub_baseline" "pubsub"
}

phys_scenario_tcp_pubsub_highrate() {
    phys_reset
    export TOPIC=data MSG_RATE=5000 MSG_SIZE=128 DURATION=30 TRANSPORT=tcp
    phys_run_scenario "phys_tcp_pubsub_highrate" "pubsub"
}

phys_scenario_tcp_pubsub_largemsg() {
    phys_reset
    export TOPIC=data MSG_RATE=200 MSG_SIZE=8192 DURATION=30 TRANSPORT=tcp
    phys_run_scenario "phys_tcp_pubsub_largemsg" "pubsub"
}

phys_scenario_tcp_pubsub_multinode() {
    export LOCAL_PUBS=10 LOCAL_SUBS=30 REMOTE_PUBS=10 REMOTE_SUBS=30
    export TOPIC=data MSG_RATE=2000 MSG_SIZE=256 DURATION=30 TRANSPORT=tcp
    phys_run_scenario "phys_tcp_pubsub_multinode" "pubsub"
}

phys_scenario_tcp_reqrep_baseline() {
    phys_reset
    export MSG_SIZE=256 DURATION=30 CONCURRENCY=5 TRANSPORT=tcp
    phys_run_scenario "phys_tcp_reqrep_baseline" "reqrep"
}

phys_scenario_tcp_reqrep_stress() {
    phys_reset
    export MSG_SIZE=256 DURATION=30 CONCURRENCY=20 TRANSPORT=tcp
    phys_run_scenario "phys_tcp_reqrep_stress" "reqrep"
}

ALL_PHYS_SCENARIOS=(
    # ── QUIC ──────────────────────────────────────────────────────────────────
    phys_pubsub_baseline
    phys_pubsub_highrate
    phys_pubsub_largemsg
    phys_pubsub_multinode
    phys_reqrep_baseline
    phys_reqrep_stress
    phys_datagram_baseline
    phys_datagram_highrate
    # ── TCP (comparison) ──────────────────────────────────────────────────────
    phys_tcp_pubsub_baseline
    phys_tcp_pubsub_highrate
    phys_tcp_pubsub_largemsg
    phys_tcp_pubsub_multinode
    phys_tcp_reqrep_baseline
    phys_tcp_reqrep_stress
)

check_phys() {
    if [[ -z "${REMOTE_HOST:-}" ]]; then
        die "REMOTE_HOST is required for phys mode.\nSet it before calling run.sh:\n  REMOTE_HOST=192.168.1.42 REMOTE_USER=pi ./run.sh phys ..."
    fi
    command -v python3 >/dev/null 2>&1 || die "python3 is required for phys mode"
    command -v ssh     >/dev/null 2>&1 || die "ssh is required for phys mode"
    command -v scp     >/dev/null 2>&1 || die "scp is required for phys mode"
    command -v go      >/dev/null 2>&1 || die "go is required to cross-compile for phys mode"
    if ! ssh -o StrictHostKeyChecking=no -o BatchMode=yes \
             "${REMOTE_USER:-pi}@${REMOTE_HOST}" true 2>/dev/null; then
        die "Cannot SSH to ${REMOTE_USER:-pi}@${REMOTE_HOST}.\nSet up key auth first:\n  ssh-copy-id ${REMOTE_USER:-pi}@${REMOTE_HOST}"
    fi
    info "SSH to ${REMOTE_USER:-pi}@${REMOTE_HOST} OK"
}

phys_run_named() {
    local name="$1"
    local fn="phys_scenario_${name#phys_}"
    if declare -f "$fn" > /dev/null; then
        "$fn"
    else
        die "Unknown phys scenario '$name'. Run '$0 phys list' to see available."
    fi
}

phys_list_scenarios() {
    printf "\nAvailable phys (physical device) scenarios:\n"
    for s in "${ALL_PHYS_SCENARIOS[@]}"; do
        printf "  %s\n" "$s"
    done
    echo
}

# ── Check mininet availability ────────────────────────────────────────────────

check_mininet() {
    if ! python3 -c "import mininet" 2>/dev/null; then
        warn "mininet Python package not found."
        printf "Install with:\n  sudo apt-get install -y mininet\n\n"
        read -r -p "Install now? [y/N] " ans
        if [[ "${ans,,}" == "y" ]]; then
            sudo apt-get install -y mininet || die "Failed to install mininet"
        else
            die "mininet is required for prod mode"
        fi
    fi
    if [[ $EUID -ne 0 ]] && ! sudo -n true 2>/dev/null; then
        warn "prod mode uses sudo to run mininet — you may be prompted for a password"
    fi
}

# ── Main dispatch ─────────────────────────────────────────────────────────────

build_image() {
    require docker
    require jq
    docker compose version >/dev/null 2>&1 || die "docker compose v2 required (not docker-compose v1)"
    info "Building quicmq-scenarios image..."
    docker build \
        -f benchmarks/scenarios/Dockerfile \
        -t quicmq-scenarios:latest \
        . 2>&1 | tail -5
    success "Image built."
}

list_scenarios() {
    printf "\nAvailable scenarios:\n"
    for s in "${ALL_SCENARIOS[@]}"; do
        printf "  %s\n" "$s"
    done
    echo
}

run_named() {
    local name="$1"
    local fn="scenario_${name}"
    if declare -f "$fn" > /dev/null; then
        "$fn"
    else
        die "Unknown scenario '$name'. Run '$0 list' to see available scenarios."
    fi
}

prod_run_named() {
    local name="$1"
    local fn="prod_scenario_${name#prod_}"  # strip leading "prod_" if present
    if declare -f "$fn" > /dev/null; then
        "$fn"
    else
        die "Unknown prod scenario '$name'. Run '$0 prod list' to see available."
    fi
}

prod_list_scenarios() {
    printf "\nAvailable prod (mininet) scenarios:\n"
    for s in "${ALL_PROD_SCENARIOS[@]}"; do
        printf "  %s\n" "$s"
    done
    echo
}

main() {
    # ── Mode detection ───────────────────────────────────────────────────────
    # First positional arg may be --help/-h or a mode (dev/prod).
    # Anything else falls through to the dev dispatcher for backward compat.

    local mode="dev"
    local args=("$@")

    if [[ ${#args[@]} -gt 0 ]]; then
        case "${args[0]}" in
        --help|-h)
            print_help; exit 0 ;;
        dev)
            mode="dev"; args=("${args[@]:1}") ;;
        prod)
            mode="prod"; args=("${args[@]:1}") ;;
        phys)
            mode="phys"; args=("${args[@]:1}") ;;
        esac
    fi

    if [[ "$mode" == "phys" ]]; then
        local cmd="${args[0]:-all}"
        case "$cmd" in
        list)
            phys_list_scenarios
            return
            ;;
        esac
        check_phys
        case "$cmd" in
        all)
            mkdir -p "$PHYS_RESULTS_ROOT"
            for s in "${ALL_PHYS_SCENARIOS[@]}"; do
                phys_run_named "$s" || warn "Phys scenario '$s' failed — continuing."
            done
            printf "\n${BOLD}All phys scenarios complete.${RESET} Results in %s/\n\n" "$PHYS_RESULTS_ROOT"
            ;;
        *)
            mkdir -p "$PHYS_RESULTS_ROOT"
            for name in "${args[@]}"; do
                phys_run_named "$name" || warn "Phys scenario '$name' failed — continuing."
            done
            ;;
        esac
        return
    fi

    if [[ "$mode" == "prod" ]]; then
        check_mininet
        local cmd="${args[0]:-all}"
        case "$cmd" in
        list)
            prod_list_scenarios
            ;;
        all)
            mkdir -p "$PROD_RESULTS_ROOT"
            for s in "${ALL_PROD_SCENARIOS[@]}"; do
                prod_run_named "$s" || warn "Prod scenario '$s' failed — continuing."
            done
            printf "\n${BOLD}All prod scenarios complete.${RESET} Results in %s/\n\n" "$PROD_RESULTS_ROOT"
            ;;
        *)
            mkdir -p "$PROD_RESULTS_ROOT"
            for name in "${args[@]}"; do
                prod_run_named "$name" || warn "Prod scenario '$name' failed — continuing."
            done
            ;;
        esac
        return
    fi

    # ── Dev mode (Docker) ────────────────────────────────────────────────────
    local cmd="${args[0]:-all}"

    case "$cmd" in
    build)
        build_image
        ;;
    list)
        list_scenarios
        ;;
    all)
        build_image
        mkdir -p "$RESULTS_ROOT"
        for s in "${ALL_SCENARIOS[@]}"; do
            run_named "$s" || warn "Scenario '$s' failed — continuing."
        done
        printf "\n${BOLD}All scenarios complete.${RESET} Results in %s/\n\n" "$RESULTS_ROOT"
        ;;
    *)
        build_image
        mkdir -p "$RESULTS_ROOT"
        for name in "${args[@]}"; do
            run_named "$name" || warn "Scenario '$name' failed — continuing."
        done
        ;;
    esac
}

main "$@"
