#!/usr/bin/env python3
"""
large_scenario.py — QuicMQ large-scale multi-node benchmark.

Creates a star/fan-out topology with up to 20 Mininet virtual hosts to
simulate realistic 1-to-many messaging deployments:

    For PUB/SUB  (N_PUBS publishers → N_SUBS subscribers):
        h1..hN_PUBS  = publisher nodes
        rest         = subscriber nodes

    For REQ/REP  (N_REPS replier nodes ← N_REQS requester nodes):
        h1..hN_REPS  = replier nodes
        rest         = requester nodes

    For Datagram (N_PUBS datagram publishers → N_SUBS datagram subs):
        same topology as PUB/SUB above

Environment variables:
    N_PUBS         publishers (default 5)
    N_SUBS         subscribers (default 15)
    N_REPS         REP servers (default 3)
    N_REQS         REQ clients (default 10)
    MSG_RATE       messages per second per publisher (default 200)
    MSG_SIZE       bytes per message (default 256)
    DURATION       run duration in seconds (default 25)
    MODE           pubsub | reqrep | datagram  (default pubsub)
    SCENARIO       label for result files (default "large")
    TOPIC          pub/sub topic (default "data")
    CONCURRENCY    REQ goroutines per client (default 3)
    NETEM_DELAY_MS, NETEM_JITTER_MS, NETEM_LOSS_PCT, NETEM_RATE_KBIT
        (applied on core uplinks only, simulating WAN degradation)

Output:
    JSON result files in $RESULTS_DIR/<scenario>/
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
    print("ERROR: mininet Python package not found. Install: sudo apt install mininet", file=sys.stderr)
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────

def env(key, default=""):
    return os.environ.get(key, default)

def env_int(key, default):
    try:
        return int(os.environ.get(key, default))
    except (ValueError, TypeError):
        return default

TOPIC       = env("TOPIC", "data")
MSG_RATE    = env_int("MSG_RATE",    200)
MSG_SIZE    = env_int("MSG_SIZE",    256)
DURATION    = env_int("DURATION",    25)
N_PUBS      = env_int("N_PUBS",      5)
N_SUBS      = env_int("N_SUBS",     15)
N_REPS      = env_int("N_REPS",      3)
N_REQS      = env_int("N_REQS",     10)
CONCURRENCY = env_int("CONCURRENCY", 3)
MODE        = env("MODE",     "pubsub")
SCENARIO    = env("SCENARIO", "large")

DELAY_MS    = env_int("NETEM_DELAY_MS",  0)
JITTER_MS   = env_int("NETEM_JITTER_MS", 0)
LOSS_PCT    = env_int("NETEM_LOSS_PCT",  0)
RATE_KBIT   = env_int("NETEM_RATE_KBIT", 0)

SCRIPT_DIR  = Path(__file__).parent
BIN_DIR     = SCRIPT_DIR.parent / "_bin"
RESULTS_DIR = Path(env("RESULTS_DIR", str(SCRIPT_DIR / "results"))) / SCENARIO


def log(msg):
    print(f"[large] {msg}", flush=True)


def _bin(name):
    p = BIN_DIR / name
    if not p.exists():
        sys.exit(f"binary not found: {p}  — run: cd repo_root && make build-bins")
    return str(p)


# ── Topology ───────────────────────────────────────────────────────────────────

class StarTopo(Topo):
    """
    Single core switch with N hosts attached.
    WAN degradation applied to all links from publisher-side hosts
    to simulate realistic network conditions.

    Layout for PUB/SUB with N_PUBS=5, N_SUBS=15  (total 20 hosts):
        h1..h5  → publisher nodes (bound on their IP)
        h6..h20 → subscriber nodes (dial pub IPs)
    """

    def build(self, n_hosts=20, delay_ms=0, jitter_ms=0, loss_pct=0, rate_kbit=0):
        self._n_hosts = n_hosts
        s0 = self.addSwitch("s0")

        lp = {}
        if delay_ms  > 0: lp["delay"]  = f"{delay_ms}ms"
        if jitter_ms > 0: lp["jitter"] = f"{jitter_ms}ms"
        if loss_pct  > 0: lp["loss"]   = loss_pct
        if rate_kbit > 0: lp["bw"]     = rate_kbit / 1000.0

        for i in range(1, n_hosts + 1):
            h = self.addHost(f"h{i}")
            if lp:
                self.addLink(h, s0, cls=TCLink, **lp)
            else:
                self.addLink(h, s0)


# ── PUB/SUB ───────────────────────────────────────────────────────────────────

def run_pubsub(net):
    pub_bin = _bin("pub")
    sub_bin = _bin("sub")

    # Start publishers
    pub_procs = []
    pub_addrs = []
    for i in range(1, N_PUBS + 1):
        h = net[f"h{i}"]
        port = 9900 + i
        addr = f"quic://{h.IP()}:{port}"
        env_str = (
            f"LISTEN_ADDR={addr} TOPIC={TOPIC} "
            f"MSG_RATE={MSG_RATE} MSG_SIZE={MSG_SIZE} "
            f"DURATION={DURATION + 6} SCENARIO={SCENARIO}"
        )
        p = h.popen(f"env {env_str} {pub_bin}", shell=True)
        pub_procs.append((f"pub-{i}", p))
        pub_addrs.append(addr)
        log(f"pub-{i}: {addr}")

    time.sleep(2)

    # Start subscribers: each connects to a publisher (round-robin)
    sub_procs = []
    for i in range(N_PUBS + 1, N_PUBS + N_SUBS + 1):
        h = net[f"h{i}"]
        sub_idx = i - N_PUBS
        target = pub_addrs[(sub_idx - 1) % len(pub_addrs)]
        env_str = (
            f"PUB_ADDR={target} TOPIC={TOPIC} "
            f"DURATION={DURATION} SCENARIO={SCENARIO} "
            f"NETEM_DELAY_MS={DELAY_MS} NETEM_LOSS_PCT={LOSS_PCT}"
        )
        p = h.popen(f"env {env_str} {sub_bin}", shell=True)
        sub_procs.append((f"sub-{sub_idx}", p))
        log(f"sub-{sub_idx} → {target}")

    log(f"pubsub running: {N_PUBS} pubs, {N_SUBS} subs for {DURATION}s…")
    time.sleep(DURATION + 4)

    outputs = []
    for name, p in pub_procs:
        out, _ = p.communicate(timeout=8)
        _collect(outputs, "pub", name, out)
        p.terminate()
    for name, p in sub_procs:
        out, _ = p.communicate(timeout=8)
        _collect(outputs, "sub", name, out)
        p.terminate()
    return outputs


# ── REQ/REP ───────────────────────────────────────────────────────────────────

def run_reqrep(net):
    rep_bin = _bin("rep")
    req_bin = _bin("req")

    rep_procs = []
    rep_addrs = []
    for i in range(1, N_REPS + 1):
        h = net[f"h{i}"]
        port = 9800 + i
        addr = f"quic://{h.IP()}:{port}"
        env_str = f"LISTEN_ADDR={addr} DURATION={DURATION + 6} SCENARIO={SCENARIO}"
        p = h.popen(f"env {env_str} {rep_bin}", shell=True)
        rep_procs.append((f"rep-{i}", p))
        rep_addrs.append(addr)
        log(f"rep-{i}: {addr}")

    time.sleep(2)

    req_procs = []
    for i in range(N_REPS + 1, N_REPS + N_REQS + 1):
        h = net[f"h{i}"]
        req_idx = i - N_REPS
        target = rep_addrs[(req_idx - 1) % len(rep_addrs)]
        env_str = (
            f"SERVER_ADDR={target} CONCURRENCY={CONCURRENCY} "
            f"MSG_SIZE={MSG_SIZE} DURATION={DURATION} SCENARIO={SCENARIO} "
            f"NETEM_DELAY_MS={DELAY_MS} NETEM_LOSS_PCT={LOSS_PCT}"
        )
        p = h.popen(f"env {env_str} {req_bin}", shell=True)
        req_procs.append((f"req-{req_idx}", p))
        log(f"req-{req_idx} → {target}")

    log(f"reqrep running: {N_REPS} reps, {N_REQS} reqs ({CONCURRENCY} goroutines each) for {DURATION}s…")
    time.sleep(DURATION + 4)

    outputs = []
    for name, p in rep_procs:
        out, _ = p.communicate(timeout=8)
        _collect(outputs, "rep", name, out)
        p.terminate()
    for name, p in req_procs:
        out, _ = p.communicate(timeout=8)
        _collect(outputs, "req", name, out)
        p.terminate()
    return outputs


# ── Datagram ──────────────────────────────────────────────────────────────────

def run_datagram(net):
    pub_bin = _bin("dpub")
    sub_bin = _bin("dsub")

    pub_procs = []
    pub_addrs = []
    for i in range(1, N_PUBS + 1):
        h = net[f"h{i}"]
        port = 9700 + i
        addr = f"quic://{h.IP()}:{port}"
        env_str = (
            f"LISTEN_ADDR={addr} TOPIC={TOPIC} "
            f"MSG_RATE={MSG_RATE} MSG_SIZE={MSG_SIZE} "
            f"DURATION={DURATION + 6} SCENARIO={SCENARIO}"
        )
        p = h.popen(f"env {env_str} {pub_bin}", shell=True)
        pub_procs.append((f"dpub-{i}", p))
        pub_addrs.append(addr)
        log(f"dpub-{i}: {addr}")

    time.sleep(2)

    sub_procs = []
    for i in range(N_PUBS + 1, N_PUBS + N_SUBS + 1):
        h = net[f"h{i}"]
        sub_idx = i - N_PUBS
        target = pub_addrs[(sub_idx - 1) % len(pub_addrs)]
        env_str = (
            f"PUB_ADDR={target} TOPIC={TOPIC} "
            f"DURATION={DURATION} SCENARIO={SCENARIO} "
            f"NETEM_DELAY_MS={DELAY_MS} NETEM_LOSS_PCT={LOSS_PCT}"
        )
        p = h.popen(f"env {env_str} {sub_bin}", shell=True)
        sub_procs.append((f"dsub-{sub_idx}", p))
        log(f"dsub-{sub_idx} → {target}")

    log(f"datagram running: {N_PUBS} dpubs, {N_SUBS} dsubs for {DURATION}s…")
    time.sleep(DURATION + 4)

    outputs = []
    for name, p in pub_procs:
        out, _ = p.communicate(timeout=8)
        _collect(outputs, "dpub", name, out)
        p.terminate()
    for name, p in sub_procs:
        out, _ = p.communicate(timeout=8)
        _collect(outputs, "dsub", name, out)
        p.terminate()
    return outputs


# ── Helpers ────────────────────────────────────────────────────────────────────

def _collect(outputs, role, name, raw_bytes):
    if not raw_bytes:
        return
    for line in raw_bytes.decode(errors="replace").splitlines():
        line = line.strip()
        if line.startswith("{"):
            outputs.append((role, name, line))


def save_outputs(outputs):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    for role, name, json_str in outputs:
        path = RESULTS_DIR / f"{name}.json"
        try:
            obj = json.loads(json_str)
        except json.JSONDecodeError:
            obj = {"raw": json_str}
        path.write_text(json.dumps(obj, indent=2))
    log(f"saved {len(outputs)} result files to {RESULTS_DIR}")


def print_summary(outputs):
    print(f"\n{'━'*70}")
    print(f"  Scenario: {SCENARIO}   mode: {MODE}   hosts: {N_PUBS + N_SUBS} virtual nodes")
    print(f"  link: delay={DELAY_MS}ms jitter={JITTER_MS}ms loss={LOSS_PCT}% bw={RATE_KBIT}kbit")
    print(f"{'━'*70}")

    pub_rates, sub_p50s, sub_p99s, sub_gaps = [], [], [], []
    req_p50s, req_p99s, rep_rates = [], [], []

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
            print(f"  {name:<16}  sent={obj.get('msgs_sent',0):<8}  rate={rate:<7.0f} msg/s  {mbps:.3f} MB/s")
        elif r in ("sub", "dsub"):
            rcvd = obj.get("msgs_received", obj.get("msgs_recv", 0))
            gaps = obj.get("seq_gaps", 0)
            p50  = obj.get("latency_p50_ms", 0)
            p99  = obj.get("latency_p99_ms", 0)
            sub_p50s.append(p50)
            sub_p99s.append(p99)
            sub_gaps.append(gaps)
            print(f"  {name:<16}  rcvd={rcvd:<8}  gaps={gaps:<5}  p50={p50:.3f}ms  p99={p99:.3f}ms")
        elif r == "rep":
            rate = obj.get("actual_rate", 0)
            rep_rates.append(rate)
            print(f"  {name:<16}  handled={obj.get('reqs_handled',0):<8}  rate={rate:.0f} req/s")
        elif r == "req":
            p50  = obj.get("rtt_p50_ms", 0)
            p99  = obj.get("rtt_p99_ms", 0)
            errs = obj.get("errors", 0)
            req_p50s.append(p50)
            req_p99s.append(p99)
            print(f"  {name:<16}  sent={obj.get('reqs_sent',0):<8}  p50={p50:.3f}ms  p99={p99:.3f}ms  err={errs}")

    print(f"\n  ── Aggregates ──────────────────────────────────────────────")
    if pub_rates:
        total = sum(pub_rates)
        avg   = total / len(pub_rates)
        print(f"  Publishers  : {len(pub_rates)} nodes  total={total:.0f} msg/s  avg={avg:.0f} msg/s")
    if sub_p50s:
        avg50 = sum(sub_p50s) / len(sub_p50s)
        avg99 = sum(sub_p99s) / len(sub_p99s)
        gaps  = sum(sub_gaps)
        print(f"  Subscribers : {len(sub_p50s)} nodes  p50={avg50:.3f}ms  p99={avg99:.3f}ms  seq_gaps={gaps}")
    if rep_rates:
        print(f"  Repliers    : {len(rep_rates)} nodes  total={sum(rep_rates):.0f} req/s")
    if req_p50s:
        avg50 = sum(req_p50s) / len(req_p50s)
        avg99 = sum(req_p99s) / len(req_p99s)
        print(f"  Requesters  : {len(req_p50s)} nodes  p50={avg50:.3f}ms  p99={avg99:.3f}ms")
    print()


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    if os.geteuid() != 0:
        sys.exit("mininet requires root — re-run with sudo")

    if MODE == "pubsub":
        n_hosts = N_PUBS + N_SUBS
    elif MODE == "datagram":
        n_hosts = N_PUBS + N_SUBS
    elif MODE == "reqrep":
        n_hosts = N_REPS + N_REQS
    else:
        sys.exit(f"unknown MODE={MODE!r}  (pubsub|reqrep|datagram)")

    setLogLevel("warning")
    cleanup()

    topo = StarTopo(
        n_hosts=n_hosts,
        delay_ms=DELAY_MS, jitter_ms=JITTER_MS,
        loss_pct=LOSS_PCT, rate_kbit=RATE_KBIT,
    )
    net = Mininet(topo=topo, link=TCLink)
    net.start()

    log(f"mode={MODE}  nodes={n_hosts}  pubs={N_PUBS}  subs={N_SUBS}  dur={DURATION}s")
    for i in range(1, n_hosts + 1):
        h = net[f"h{i}"]
        log(f"  h{i} = {h.IP()}")

    try:
        if MODE == "pubsub":
            outputs = run_pubsub(net)
        elif MODE == "reqrep":
            outputs = run_reqrep(net)
        else:
            outputs = run_datagram(net)

        save_outputs(outputs)
        print_summary(outputs)
    finally:
        net.stop()
        cleanup()


if __name__ == "__main__":
    main()
