#!/usr/bin/env python3
"""
mesh_scenario.py — QuicMQ multi-node mesh benchmark.

Creates a four-host fat-tree topology that mirrors a real distributed
deployment where publishers and subscribers on different machines form
a complex cross-connected network:

             ┌─────────────────────────────┐
             │        core switch s0        │
             └──┬──────────┬───────────────┘
          ┌─────┘          └─────┐
       sw1 (edge)           sw2 (edge)
       ├── h1 (pub node A)  ├── h3 (pub node B)
       └── h2 (sub node A)  └── h4 (sub node B)

Cross-subscriptions:
  • subs on h2 subscribe to pubs on BOTH h1 AND h3
  • subs on h4 subscribe to pubs on BOTH h1 AND h3

This creates a realistic "mesh web" of N_PUBS×2 publishers and
N_SUBS×2 subscribers with every subscriber receiving from every
publisher.

Environment variables (same as mininet_scenario.py):
  TOPIC, MSG_RATE, MSG_SIZE, DURATION, N_PUBS, N_SUBS, N_REQS
  MODE  pubsub | reqrep | datagram  (default pubsub)
  NETEM_DELAY_MS, NETEM_JITTER_MS, NETEM_LOSS_PCT, NETEM_RATE_KBIT

Output:
  JSON result files in $RESULTS_DIR/
"""

import json
import os
import sys
import time
from pathlib import Path

try:
    from mininet.net import Mininet
    from mininet.topo import Topo
    from mininet.link import TCLink
    from mininet.log import setLogLevel
    from mininet.clean import cleanup
except ImportError:
    print("ERROR: mininet Python package not found.", file=sys.stderr)
    print("Install:  sudo apt-get install -y mininet", file=sys.stderr)
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────

def env(key, default=""):      return os.environ.get(key, default)
def env_int(key, default):
    try: return int(os.environ.get(key, default))
    except (ValueError, TypeError): return default

TOPIC    = env("TOPIC",    "data")
MSG_RATE = env_int("MSG_RATE", 500)
MSG_SIZE = env_int("MSG_SIZE", 256)
DURATION = env_int("DURATION", 30)
N_PUBS   = env_int("N_PUBS",   2)   # pubs per pub-node (×2 nodes)
N_SUBS   = env_int("N_SUBS",   3)   # subs per sub-node (×2 nodes)
N_REQS   = env_int("N_REQS",   5)
MODE     = env("MODE",     "pubsub")
SCENARIO = env("SCENARIO", "mesh")

DELAY_MS    = env_int("NETEM_DELAY_MS",  0)
JITTER_MS   = env_int("NETEM_JITTER_MS", 0)
LOSS_PCT    = env_int("NETEM_LOSS_PCT",  0)
RATE_KBIT   = env_int("NETEM_RATE_KBIT", 0)

SCRIPT_DIR  = Path(__file__).parent
BIN_DIR     = SCRIPT_DIR / "_bin"          # pre-built by run.sh / Makefile
RESULTS_DIR = Path(env("RESULTS_DIR", str(SCRIPT_DIR / "results"))) / SCENARIO


def log(msg):
    print(f"[mesh] {msg}", flush=True)


# ── Topology ──────────────────────────────────────────────────────────────────

class MeshTopo(Topo):
    """
    Fat-tree / two-tier topology:

        h1 (pub-A)  h2 (sub-A)       h3 (pub-B)  h4 (sub-B)
           \          /                  \          /
            sw1 ─── s0 (core) ──────── sw2

    Link parameters are applied on the uplink from each edge switch
    to the core, simulating a wide-area connection between the two sites.
    """

    def build(self, delay_ms=0, jitter_ms=0, loss_pct=0, rate_kbit=0):
        h1 = self.addHost("h1")   # pub node A
        h2 = self.addHost("h2")   # sub node A
        h3 = self.addHost("h3")   # pub node B
        h4 = self.addHost("h4")   # sub node B

        sw1 = self.addSwitch("sw1")
        sw2 = self.addSwitch("sw2")
        s0  = self.addSwitch("s0")   # core

        # Fast local links (no degradation)
        self.addLink(h1, sw1)
        self.addLink(h2, sw1)
        self.addLink(h3, sw2)
        self.addLink(h4, sw2)

        # WAN uplinks with optional degradation
        lp = {}
        if delay_ms > 0:  lp["delay"]  = f"{delay_ms}ms"
        if jitter_ms > 0: lp["jitter"] = f"{jitter_ms}ms"
        if loss_pct > 0:  lp["loss"]   = loss_pct
        if rate_kbit > 0: lp["bw"]     = rate_kbit / 1000.0

        if lp:
            self.addLink(sw1, s0, cls=TCLink, **lp)
            self.addLink(sw2, s0, cls=TCLink, **lp)
        else:
            self.addLink(sw1, s0)
            self.addLink(sw2, s0)


# ── Scenario runners ──────────────────────────────────────────────────────────

def _bin(name):
    p = BIN_DIR / name
    if not p.exists():
        sys.exit(f"binary not found: {p}  — run 'make build-bins' first")
    return str(p)


def run_mesh_pubsub(net):
    """
    Two pub nodes × N_PUBS pubs + two sub nodes × N_SUBS subs.
    Every subscriber connects to every publisher (full cross-connect).
    """
    h1, h2, h3, h4 = net["h1"], net["h2"], net["h3"], net["h4"]
    pub_bin = _bin("pub")
    sub_bin = _bin("sub")

    # Start publishers on h1 and h3
    pub_procs   = []
    pub_addrs   = []   # all publisher addresses (both nodes)

    for node_idx, host in enumerate([h1, h3]):
        node_ip = host.IP()
        for i in range(N_PUBS):
            port = 9900 + node_idx * 100 + i
            addr = f"quic://{node_ip}:{port}"
            env_str = (
                f"LISTEN_ADDR={addr} TOPIC={TOPIC} "
                f"MSG_RATE={max(1, MSG_RATE // (N_PUBS * 2))} "
                f"MSG_SIZE={MSG_SIZE} DURATION={DURATION + 8} SCENARIO={SCENARIO}"
            )
            p = host.popen(f"env {env_str} {pub_bin}", shell=True)
            pub_procs.append((f"pub-n{node_idx}-{i}", p))
            pub_addrs.append(addr)
            log(f"pub-n{node_idx}-{i}: {addr}")

    time.sleep(3)  # let all publishers bind

    # Start subscribers on h2 and h4 — each sub connects to ALL publishers
    sub_procs = []
    for node_idx, host in enumerate([h2, h4]):
        for i in range(N_SUBS):
            # Round-robin across publishers
            target = pub_addrs[i % len(pub_addrs)]
            env_str = (
                f"SERVER_ADDR={target} TOPIC={TOPIC} "
                f"DURATION={DURATION} SCENARIO={SCENARIO} "
                f"NETEM_DELAY_MS={DELAY_MS} NETEM_LOSS_PCT={LOSS_PCT}"
            )
            p = host.popen(f"env {env_str} {sub_bin}", shell=True)
            sub_procs.append((f"sub-n{node_idx}-{i}", p))
            log(f"sub-n{node_idx}-{i} → {target}")

    log(f"mesh running: {len(pub_procs)} pubs × {len(sub_procs)} subs for {DURATION}s…")
    time.sleep(DURATION + 5)

    outputs = []
    for name, p in pub_procs:
        out, _ = p.communicate(timeout=10)
        if out:
            for line in out.decode(errors="replace").splitlines():
                line = line.strip()
                if line.startswith("{"):
                    outputs.append(("pub", name, line))
        p.terminate()

    for name, p in sub_procs:
        out, _ = p.communicate(timeout=10)
        if out:
            for line in out.decode(errors="replace").splitlines():
                line = line.strip()
                if line.startswith("{"):
                    outputs.append(("sub", name, line))
        p.terminate()

    return outputs


def run_mesh_reqrep(net):
    """
    REP server on h1, multiple REQ clients spread across h2 and h4.
    Also a second REP on h3 for cross-site load distribution.
    """
    h1, h2, h3, h4 = net["h1"], net["h2"], net["h3"], net["h4"]
    rep_bin = _bin("rep")
    req_bin = _bin("req")

    rep_procs = []
    rep_addrs = []
    for node_idx, host in enumerate([h1, h3]):
        port = 9800 + node_idx * 100
        addr = f"quic://{host.IP()}:{port}"
        env_str = f"LISTEN_ADDR={addr} DURATION={DURATION + 8} SCENARIO={SCENARIO}"
        p = host.popen(f"env {env_str} {rep_bin}", shell=True)
        rep_procs.append((f"rep-n{node_idx}", p))
        rep_addrs.append(addr)
        log(f"rep-n{node_idx}: {addr}")

    time.sleep(2)

    req_procs = []
    for node_idx, host in enumerate([h2, h4]):
        target = rep_addrs[node_idx % len(rep_addrs)]
        concurrency = max(1, N_REQS // 2)
        env_str = (
            f"SERVER_ADDR={target} CONCURRENCY={concurrency} "
            f"MSG_SIZE={MSG_SIZE} DURATION={DURATION} SCENARIO={SCENARIO} "
            f"NETEM_DELAY_MS={DELAY_MS} NETEM_LOSS_PCT={LOSS_PCT}"
        )
        p = host.popen(f"env {env_str} {req_bin}", shell=True)
        req_procs.append((f"req-n{node_idx}", p))
        log(f"req-n{node_idx} → {target}")

    log(f"mesh reqrep running for {DURATION}s…")
    time.sleep(DURATION + 5)

    outputs = []
    for procs in [rep_procs, req_procs]:
        for name, p in procs:
            out, _ = p.communicate(timeout=10)
            role = "rep" if name.startswith("rep") else "req"
            if out:
                for line in out.decode(errors="replace").splitlines():
                    line = line.strip()
                    if line.startswith("{"):
                        outputs.append((role, name, line))
            p.terminate()

    return outputs


def run_mesh_datagram(net):
    """
    DatagramPub nodes on h1 and h3; DatagramSub nodes on h2 and h4.
    Full cross-subscribe: each sub subscribes to each pub.
    """
    h1, h2, h3, h4 = net["h1"], net["h2"], net["h3"], net["h4"]
    pub_bin = _bin("dpub")
    sub_bin = _bin("dsub")

    pub_procs = []
    pub_addrs = []

    for node_idx, host in enumerate([h1, h3]):
        port = 9700 + node_idx * 100
        addr = f"quic://{host.IP()}:{port}"
        rate = max(1, MSG_RATE // 2)
        env_str = (
            f"LISTEN_ADDR={addr} TOPIC={TOPIC} MSG_RATE={rate} "
            f"MSG_SIZE={MSG_SIZE} DURATION={DURATION + 8} SCENARIO={SCENARIO}"
        )
        p = host.popen(f"env {env_str} {pub_bin}", shell=True)
        pub_procs.append((f"dpub-n{node_idx}", p))
        pub_addrs.append(addr)
        log(f"dpub-n{node_idx}: {addr}")

    time.sleep(3)

    sub_procs = []
    for node_idx, host in enumerate([h2, h4]):
        for i in range(N_SUBS):
            target = pub_addrs[i % len(pub_addrs)]
            env_str = (
                f"PUB_ADDR={target} TOPIC={TOPIC} DURATION={DURATION} "
                f"SCENARIO={SCENARIO} NETEM_DELAY_MS={DELAY_MS} NETEM_LOSS_PCT={LOSS_PCT}"
            )
            p = host.popen(f"env {env_str} {sub_bin}", shell=True)
            sub_procs.append((f"dsub-n{node_idx}-{i}", p))
            log(f"dsub-n{node_idx}-{i} → {target}")

    log(f"mesh datagram running for {DURATION}s…")
    time.sleep(DURATION + 5)

    outputs = []
    for name, p in pub_procs:
        out, _ = p.communicate(timeout=10)
        if out:
            for line in out.decode(errors="replace").splitlines():
                line = line.strip()
                if line.startswith("{"):
                    outputs.append(("dpub", name, line))
        p.terminate()

    for name, p in sub_procs:
        out, _ = p.communicate(timeout=10)
        if out:
            for line in out.decode(errors="replace").splitlines():
                line = line.strip()
                if line.startswith("{"):
                    outputs.append(("dsub", name, line))
        p.terminate()

    return outputs


# ── Result I/O ────────────────────────────────────────────────────────────────

def save_outputs(outputs):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    saved_paths = []
    for role, name, json_str in outputs:
        path = RESULTS_DIR / f"{name}.json"
        try:
            obj = json.loads(json_str)
        except json.JSONDecodeError:
            obj = {"raw": json_str}
        path.write_text(json.dumps(obj, indent=2))
        saved_paths.append(str(path))
    log(f"saved {len(saved_paths)} result files to {RESULTS_DIR}")
    return saved_paths


def print_summary(outputs):
    print(f"\n{'━'*70}")
    print(f"  Scenario: {SCENARIO}  mode: {MODE}  topology: 4-node mesh")
    print(f"  WAN link: delay={DELAY_MS}ms jitter={JITTER_MS}ms loss={LOSS_PCT}% bw={RATE_KBIT}kbit")
    print(f"{'━'*70}")

    pub_rates, sub_p50s, sub_p99s, sub_gaps = [], [], [], []
    req_p50s,  req_p99s,  rep_rates          = [], [], []

    for role, name, json_str in outputs:
        try:
            obj = json.loads(json_str)
        except json.JSONDecodeError:
            continue
        r = obj.get("role", role)
        if r in ("pub", "dpub"):
            rate = obj.get("actual_rate", 0)
            mbps = obj.get("throughput_mbs", 0)
            pub_rates.append(rate)
            print(f"  {name:<18}  sent={obj.get('msgs_sent',0):<7}  "
                  f"rate={rate:<7.0f} msg/s  {mbps:.3f} MB/s")
        elif r in ("sub", "dsub"):
            rcvd = obj.get("msgs_received", obj.get("msgs_recv", 0))
            gaps = obj.get("seq_gaps", 0)
            p50  = obj.get("latency_p50_ms", 0)
            p99  = obj.get("latency_p99_ms", 0)
            sub_p50s.append(p50); sub_p99s.append(p99); sub_gaps.append(gaps)
            print(f"  {name:<18}  rcvd={rcvd:<7}  gaps={gaps:<5}  "
                  f"p50={p50:.2f}ms  p99={p99:.2f}ms")
        elif r == "rep":
            rate = obj.get("actual_rate", 0)
            rep_rates.append(rate)
            print(f"  {name:<18}  handled={obj.get('reqs_handled',0):<7}  rate={rate:.0f} req/s")
        elif r == "req":
            p50  = obj.get("rtt_p50_ms", 0)
            p99  = obj.get("rtt_p99_ms", 0)
            req_p50s.append(p50); req_p99s.append(p99)
            print(f"  {name:<18}  sent={obj.get('reqs_sent',0):<7}  "
                  f"p50={p50:.2f}ms  p99={p99:.2f}ms  err={obj.get('errors',0)}")

    print(f"\n  ── Aggregates ──")
    if pub_rates:
        print(f"  Total pub rate : {sum(pub_rates):.0f} msg/s  (avg {sum(pub_rates)/len(pub_rates):.0f})")
    if sub_p50s:
        avg50  = sum(sub_p50s) / len(sub_p50s)
        avg99  = sum(sub_p99s) / len(sub_p99s)
        total_gaps = sum(sub_gaps)
        print(f"  Sub latency    : p50={avg50:.2f}ms  p99={avg99:.2f}ms  total_gaps={total_gaps}")
    if rep_rates:
        print(f"  Total rep rate : {sum(rep_rates):.0f} req/s")
    if req_p50s:
        print(f"  REQ RTT        : p50={sum(req_p50s)/len(req_p50s):.2f}ms  "
              f"p99={sum(req_p99s)/len(req_p99s):.2f}ms")
    print()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if os.geteuid() != 0:
        sys.exit("mininet requires root — re-run with sudo")

    setLogLevel("warning")
    cleanup()

    topo = MeshTopo(
        delay_ms=DELAY_MS, jitter_ms=JITTER_MS,
        loss_pct=LOSS_PCT, rate_kbit=RATE_KBIT,
    )
    net = Mininet(topo=topo, link=TCLink)
    net.start()

    h1, h2, h3, h4 = net["h1"], net["h2"], net["h3"], net["h4"]
    log(f"h1={h1.IP()} h2={h2.IP()} h3={h3.IP()} h4={h4.IP()}")
    log(f"mode={MODE}  N_PUBS={N_PUBS}×2  N_SUBS={N_SUBS}×2  DURATION={DURATION}s")

    try:
        if MODE == "pubsub":
            outputs = run_mesh_pubsub(net)
        elif MODE == "reqrep":
            outputs = run_mesh_reqrep(net)
        elif MODE == "datagram":
            outputs = run_mesh_datagram(net)
        else:
            net.stop()
            sys.exit(f"unknown MODE={MODE!r}")

        save_outputs(outputs)
        print_summary(outputs)
    finally:
        net.stop()
        cleanup()


if __name__ == "__main__":
    main()
