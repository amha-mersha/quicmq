#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# QuicMQ Demo Build Script
#
# Builds all four demo server/client pairs:
#   • Servers → ARM64 Linux (for Raspberry Pi 5)
#   • Clients → native platform (for the laptop running this script)
#
# USAGE (from the repo root):
#   cd /path/to/quicmq
#   bash benchmarks/demos/build.sh
#
# OUTPUT:
#   benchmarks/demos/bin/
#     pi/   demo1_server  demo2_server  demo3_server  demo4_pub   demo5_server  ← copy to Pi
#     local/ demo1_client  demo2_client  demo3_client  demo4_sub  demo5_client  ← run on laptop
#
# COPY TO PI (replace 192.168.1.5 with your Pi's IP):
#   scp benchmarks/demos/bin/pi/* pi@192.168.1.5:~/quicmq_demos/
#
# ── RUNNING ORDER ────────────────────────────────────────────────────────────
#
# Demo 1 – REQ/REP Tail Latency
#   Pi:     ./demo1_server
#   Laptop: SERVER_ADDR=<pi_ip> ./demo1_client
#
# Demo 2 – Connection Migration
#   Pi:     ./demo2_server
#   Laptop: SERVER_ADDR=<pi_ip> ./demo2_client
#
# Demo 3 – Connection Pool
#   Pi:     ./demo3_server
#   Laptop: SERVER_ADDR=<pi_ip> ./demo3_client
#
# Demo 4 – PUB/SUB Resilience + Topic Filtering
#   Laptop: SERVER_ADDR=<pi_ip> ./demo4_sub   (start subscriber FIRST)
#   Pi:     ./demo4_pub                        (then start publisher)
#   Pi:     Ctrl+C demo4_pub                   (watch reconnection on laptop)
#   Pi:     ./demo4_pub                         (watch reconnect + resume)
#
# Demo 5 – 0-RTT Session Resumption (QUIC-only)
#   Pi:     ./demo5_server
#   Laptop: SERVER_ADDR=<pi_ip> ./demo5_client
#
# ── PORTS USED ───────────────────────────────────────────────────────────────
#   Demo 1:  UDP 7001 (QUIC REP),  TCP 7002 (TCP REP)
#   Demo 2:  UDP 7003 (QUIC REP)
#   Demo 3:  UDP 7005 (QUIC PUB)
#   Demo 4:  UDP 7007 (QUIC PUB)
#   Demo 5:  UDP 7009 (QUIC REP),  TCP 7010 (TCP REP)
#
# Pi firewall (run once):
#   sudo ufw allow 7001/udp && sudo ufw allow 7002/tcp
#   sudo ufw allow 7003/udp && sudo ufw allow 7005/udp && sudo ufw allow 7007/udp
#   sudo ufw allow 7009/udp && sudo ufw allow 7010/tcp
# ─────────────────────────────────────────────────────────────────────────────

set -e

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN_DIR="$REPO_ROOT/benchmarks/demos/bin"
PI_BIN="$BIN_DIR/pi"
LOCAL_BIN="$BIN_DIR/local"

mkdir -p "$PI_BIN" "$LOCAL_BIN"

echo ""
echo "Building demo binaries..."
echo "  Repo root: $REPO_ROOT"
echo "  Pi bin:    $PI_BIN"
echo "  Local bin: $LOCAL_BIN"
echo ""

cd "$REPO_ROOT"

# ── Server binaries (ARM64 Linux for Pi) ─────────────────────────────────────
build_pi() {
  local name=$1
  local pkg=$2
  echo "  [PI  ] $name  ← $pkg"
  GOARCH=arm64 GOOS=linux go build -o "$PI_BIN/$name" "./$pkg"
}

build_pi demo1_server benchmarks/demos/01_reqrep_latency/server
build_pi demo2_server benchmarks/demos/02_migration/server
build_pi demo3_server benchmarks/demos/03_pool/server
build_pi demo4_pub    benchmarks/demos/04_pubsub_resilience/publisher
build_pi demo5_server benchmarks/demos/05_0rtt/server

# ── Client binaries (native platform for laptop) ─────────────────────────────
build_local() {
  local name=$1
  local pkg=$2
  echo "  [LOCAL] $name  ← $pkg"
  go build -o "$LOCAL_BIN/$name" "./$pkg"
}

build_local demo1_client benchmarks/demos/01_reqrep_latency/client
build_local demo2_client benchmarks/demos/02_migration/client
build_local demo3_client benchmarks/demos/03_pool/client
build_local demo4_sub    benchmarks/demos/04_pubsub_resilience/subscriber
build_local demo5_client benchmarks/demos/05_0rtt/client

echo ""
echo "Done."
echo ""
echo "Next: copy Pi binaries to the Pi:"
echo "  scp $PI_BIN/* pi@<PI_IP>:~/quicmq_demos/"
echo ""
echo "Then open two terminal windows:"
echo "  window 1 (Pi ssh):  cd ~/quicmq_demos && ./demo1_server"
echo "  window 2 (laptop):  cd $LOCAL_BIN && SERVER_ADDR=<PI_IP> ./demo1_client"
