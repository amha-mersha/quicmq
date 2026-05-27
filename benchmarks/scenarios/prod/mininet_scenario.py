#!/usr/bin/env python3
"""
mininet_scenario.py — QuicMQ prod-mode scenario runner.

Creates a two-host mininet topology that mirrors a real multi-machine setup:

  h1 (pub/rep server)  ─── link ───  h2 (sub/req clients)

Link parameters are taken from environment variables so the same script
covers baseline, lossy, high-latency, and bandwidth-limited scenarios:

  NETEM_DELAY_MS    one-way delay in ms        (default 0)
  NETEM_JITTER_MS   jitter in ms               (default 0)
  NETEM_LOSS_PCT    packet loss percentage      (default 0)
  NETEM_RATE_KBIT   bandwidth cap in kbit/s     (default 0 = unlimited)
  NETEM_REORDER_PCT reorder percentage          (default 0)

Scenario parameters:
  TOPIC         pub/sub topic              (default "data")
  MSG_RATE      messages per second        (default 500)
  MSG_SIZE      bytes per message          (default 256)
  DURATION      run duration in seconds    (default 30)
  N_PUBS        publishers on h1           (default 1)
  N_SUBS        subscribers on h2          (default 3)
  N_REQS        concurrent REQ workers     (default 5)
  MODE          pubsub | reqrep | datagram  (default pubsub)
  SCENARIO      label for result files     (default "prod")

Output:
  JSON result files written to  $RESULTS_DIR/<scenario>/<role>-<n>.json
  (RESULTS_DIR defaults to ./results next to this script)

Usage (called by run.sh):
  sudo python3 mininet_scenario.py
"""

import json
import os
import sys
import time
import threading
from pathlib import Path

try:
    from mininet.net import Mininet
    from mininet.topo import Topo
    from mininet.link import TCLink
    from mininet.log import setLogLevel
    from mininet.clean import cleanup
except ImportError:
    print("ERROR: mininet Python package not found.", file=sys.stderr)
    print("Install with:  sudo apt-get install -y mininet", file=sys.stderr)
    sys.exit(1)

# ── Configuration from environment ────────────────────────────────────────────

def env(key, default=""):
    return os.environ.get(key, default)

def env_int(key, default):
    try:
        return int(os.environ.get(key, default))
    except ValueError:
        return default

TOPIC     = env("TOPIC", "data")
MSG_RATE  = env_int("MSG_RATE", 500)
MSG_SIZE  = env_int("MSG_SIZE", 256)
DURATION  = env_int("DURATION", 30)
N_PUBS    = env_int("N_PUBS", 1)
N_SUBS    = env_int("N_SUBS", 3)
N_REQS    = env_int("N_REQS", 5)
MODE      = env("MODE", "pubsub")    # pubsub | reqrep | datagram
SCENARIO  = env("SCENARIO", "prod")
TRANSPORT = env("TRANSPORT", "quic")  # quic | tcp — scheme used in all addresses

DELAY_MS    = env_int("NETEM_DELAY_MS", 0)
JITTER_MS   = env_int("NETEM_JITTER_MS", 0)
LOSS_PCT    = env_int("NETEM_LOSS_PCT", 0)
RATE_KBIT   = env_int("NETEM_RATE_KBIT", 0)
REORDER_PCT = env_int("NETEM_REORDER_PCT", 0)

SCRIPT_DIR  = Path(__file__).parent
RESULTS_DIR = Path(env("RESULTS_DIR", str(SCRIPT_DIR / "results"))) / SCENARIO
BIN_DIR     = SCRIPT_DIR.parent / "cmd"   # benchmarks/scenarios/cmd/<role>/main.go

# ── Helpers ────────────────────────────────────────────────────────────────────

def log(msg):
    print(f"[mininet] {msg}", flush=True)

def build_binary(role):
    """Build a scenario binary and return its path."""
    import subprocess
    src = str(BIN_DIR / role)
    out = str(SCRIPT_DIR / f"_bin/{role}")
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    log(f"building {role} → {out}")
    result = subprocess.run(
        ["go", "build", "-o", out, f"./{BIN_DIR / role}"],
        cwd=str(BIN_DIR.parent.parent.parent),  # repo root
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        sys.exit(f"failed to build {role}")
    return out

# ── Topology ───────────────────────────────────────────────────────────────────

class TwoHostTopo(Topo):
    """h1 ── switch ── h2 with optional TC link parameters."""

    def build(self, delay_ms=0, jitter_ms=0, loss_pct=0, rate_kbit=0, reorder_pct=0):
        h1 = self.addHost("h1")
        h2 = self.addHost("h2")
        s1 = self.addSwitch("s1")

        link_params = {}
        if delay_ms > 0:
            link_params["delay"] = f"{delay_ms}ms"
        if jitter_ms > 0:
            link_params["jitter"] = f"{jitter_ms}ms"
        if loss_pct > 0:
            link_params["loss"] = loss_pct
        if rate_kbit > 0:
            link_params["bw"] = rate_kbit / 1000.0  # TCLink expects Mbit/s

        if link_params:
            self.addLink(h1, s1, cls=TCLink, **link_params)
        else:
            self.addLink(h1, s1)
        self.addLink(s1, h2)

# ── Scenario runners ───────────────────────────────────────────────────────────

def run_pubsub(net, h1, h2, binaries):
    """Run N_PUBS publishers on h1 and N_SUBS subscribers on h2."""
    pub_bin = binaries["pub"]
    sub_bin = binaries["sub"]

    h1_ip = h1.IP()
    pub_procs = []
    pub_addrs = []

    for i in range(N_PUBS):
        port = 9900 + i
        addr = f"{TRANSPORT}://{h1_ip}:{port}"
        env_str = (
            f"LISTEN_ADDR={addr} "
            f"TOPIC={TOPIC} MSG_RATE={MSG_RATE // N_PUBS} MSG_SIZE={MSG_SIZE} "
            f"DURATION={DURATION + 5} SCENARIO={SCENARIO} TRANSPORT={TRANSPORT}"
        )
        p = h1.popen(f"env {env_str} {pub_bin}", shell=True)
        pub_procs.append(p)
        pub_addrs.append(addr)
        log(f"pub-{i}: {addr}")

    time.sleep(2)  # let publishers start

    sub_procs = []
    for i in range(N_SUBS):
        addr = pub_addrs[i % len(pub_addrs)]
        env_str = (
            f"SERVER_ADDR={addr} TOPIC={TOPIC} "
            f"DURATION={DURATION} SCENARIO={SCENARIO} TRANSPORT={TRANSPORT} "
            f"NETEM_DELAY_MS={DELAY_MS} NETEM_LOSS_PCT={LOSS_PCT}"
        )
        p = h2.popen(f"env {env_str} {sub_bin}", shell=True)
        sub_procs.append(p)
        log(f"sub-{i}: → {addr}")

    log(f"running for {DURATION}s…")
    time.sleep(DURATION + 3)

    outputs = []
    for i, p in enumerate(pub_procs):
        out, _ = p.communicate(timeout=5)
        if out:
            outputs.append(("pub", i, out.decode(errors="replace")))
        p.terminate()

    for i, p in enumerate(sub_procs):
        out, _ = p.communicate(timeout=5)
        if out:
            outputs.append(("sub", i, out.decode(errors="replace")))
        p.terminate()

    return outputs

def run_reqrep(net, h1, h2, binaries):
    """Run REP server on h1 and N_REQS concurrent REQ clients on h2."""
    rep_bin = binaries["rep"]
    req_bin = binaries["req"]

    h1_ip  = h1.IP()
    rep_addr = f"{TRANSPORT}://{h1_ip}:9800"
    env_str  = f"LISTEN_ADDR={rep_addr} DURATION={DURATION + 5} SCENARIO={SCENARIO} TRANSPORT={TRANSPORT}"
    rep_proc = h1.popen(f"env {env_str} {rep_bin}", shell=True)
    log(f"rep: {rep_addr}")

    time.sleep(2)

    req_env = (
        f"SERVER_ADDR={rep_addr} CONCURRENCY={N_REQS} "
        f"DURATION={DURATION} SCENARIO={SCENARIO} TRANSPORT={TRANSPORT} "
        f"NETEM_DELAY_MS={DELAY_MS} NETEM_LOSS_PCT={LOSS_PCT}"
    )
    req_proc = h2.popen(f"env {req_env} {req_bin}", shell=True)
    log(f"req: {N_REQS} concurrent workers → {rep_addr}")

    time.sleep(DURATION + 3)

    outputs = []
    for proc, role, idx in [(rep_proc, "rep", 0), (req_proc, "req", 0)]:
        out, _ = proc.communicate(timeout=5)
        if out:
            outputs.append((role, idx, out.decode(errors="replace")))
        proc.terminate()

    return outputs

def run_datagram(net, h1, h2, binaries):
    """Run datagram pub on h1 and N_SUBS datagram subs on h2."""
    pub_bin = binaries["dpub"]
    sub_bin = binaries["dsub"]

    h1_ip  = h1.IP()
    addr   = f"quic://{h1_ip}:9700"  # datagrams are QUIC-only; TRANSPORT ignored here
    env_str = (
        f"LISTEN_ADDR={addr} TOPIC={TOPIC} MSG_RATE={MSG_RATE} "
        f"MSG_SIZE={MSG_SIZE} DURATION={DURATION + 5} SCENARIO={SCENARIO}"
    )
    pub_proc = h1.popen(f"env {env_str} {pub_bin}", shell=True)
    log(f"dpub: {addr}")

    time.sleep(2)

    sub_procs = []
    for i in range(N_SUBS):
        sub_env = (
            f"SERVER_ADDR={addr} TOPIC={TOPIC} DURATION={DURATION} "
            f"SCENARIO={SCENARIO} NETEM_DELAY_MS={DELAY_MS} NETEM_LOSS_PCT={LOSS_PCT}"
        )
        p = h2.popen(f"env {sub_env} {sub_bin}", shell=True)
        sub_procs.append(p)
    log(f"{N_SUBS} datagram subs → {addr}")

    time.sleep(DURATION + 3)

    outputs = []
    out, _ = pub_proc.communicate(timeout=5)
    if out:
        outputs.append(("dpub", 0, out.decode(errors="replace")))
    pub_proc.terminate()

    for i, p in enumerate(sub_procs):
        out, _ = p.communicate(timeout=5)
        if out:
            outputs.append(("dsub", i, out.decode(errors="replace")))
        p.terminate()

    return outputs

# ── Result collection ──────────────────────────────────────────────────────────

def save_outputs(outputs):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    for role, idx, text in outputs:
        path = RESULTS_DIR / f"{role}-{idx}.jsonl"
        path.write_text(text)
        log(f"saved {path}")

def print_summary(outputs):
    print(f"\n{'━'*60}")
    print(f"  Scenario: {SCENARIO}  mode: {MODE}  transport: {TRANSPORT}  host: mininet")
    print(f"  link: delay={DELAY_MS}ms jitter={JITTER_MS}ms loss={LOSS_PCT}% bw={RATE_KBIT}kbit")
    print(f"{'━'*60}")
    for role, idx, text in outputs:
        for line in text.splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            r = obj.get("role", role)
            if r in ("pub", "dpub"):
                sent = obj.get("msgs_sent", 0)
                rate = obj.get("actual_rate", 0)
                mbps = obj.get("throughput_mbs", 0)
                print(f"  {r:<6}  sent={sent:<8}  rate={rate:<6.0f} msg/s  {mbps:.2f} MB/s")
            elif r in ("sub", "dsub"):
                rcvd = obj.get("msgs_received", obj.get("msgs_recv", 0))
                gaps = obj.get("seq_gaps", 0)
                p50  = obj.get("latency_p50_ms", 0)
                p99  = obj.get("latency_p99_ms", 0)
                print(f"  {r:<6}  rcvd={rcvd:<8}  gaps={gaps:<5}  p50={p50:.2f}ms  p99={p99:.2f}ms")
            elif r == "rep":
                hdl  = obj.get("reqs_handled", 0)
                rate = obj.get("actual_rate", 0)
                print(f"  rep     handled={hdl:<8}  rate={rate:.0f} req/s")
            elif r == "req":
                sent = obj.get("reqs_sent", 0)
                p50  = obj.get("rtt_p50_ms", 0)
                p99  = obj.get("rtt_p99_ms", 0)
                errs = obj.get("errors", 0)
                print(f"  req     sent={sent:<8}  p50={p50:.2f}ms  p99={p99:.2f}ms  err={errs}")
    print()

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    setLogLevel("warning")
    cleanup()

    log(f"building binaries…")
    roles = {"pubsub": ["pub", "sub"], "reqrep": ["rep", "req"], "datagram": ["dpub", "dsub"]}
    needed = roles.get(MODE, ["pub", "sub"])
    binaries = {r: build_binary(r) for r in needed}

    topo = TwoHostTopo(
        delay_ms=DELAY_MS, jitter_ms=JITTER_MS,
        loss_pct=LOSS_PCT, rate_kbit=RATE_KBIT,
        reorder_pct=REORDER_PCT,
    )
    net = Mininet(topo=topo, link=TCLink)
    net.start()

    h1 = net["h1"]
    h2 = net["h2"]

    log(f"h1={h1.IP()}  h2={h2.IP()}  mode={MODE}")

    try:
        if MODE == "pubsub":
            outputs = run_pubsub(net, h1, h2, binaries)
        elif MODE == "reqrep":
            outputs = run_reqrep(net, h1, h2, binaries)
        elif MODE == "datagram":
            outputs = run_datagram(net, h1, h2, binaries)
        else:
            sys.exit(f"unknown MODE={MODE!r} (pubsub|reqrep|datagram)")

        save_outputs(outputs)
        print_summary(outputs)

    finally:
        net.stop()
        cleanup()

if __name__ == "__main__":
    if os.geteuid() != 0:
        sys.exit("mininet requires root — re-run with sudo")
    main()
