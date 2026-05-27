#!/usr/bin/env python3
"""
phys_scenario.py — QuicMQ physical distributed benchmark runner.

Orchestrates benchmark processes across two physical machines (local laptop
and remote Raspberry Pi) over a real network connection.  Both machines
simultaneously run publishers and subscribers; subscribers are distributed
round-robin across all publishers on both machines.

Topology (pubsub mode, default):
  Laptop                             Raspberry Pi
  ──────────────────────             ──────────────────────
  LOCAL_PUBS × pub  ──────────────→  REMOTE_SUBS × sub
  LOCAL_SUBS  × sub ←──────────────  REMOTE_PUBS × pub

Setup requirements:
  - SSH key auth to remote (no password prompts): ssh-copy-id pi@<ip>
  - Go installed on both machines
  - This repo present on the local machine (binaries cross-compiled here)

Configuration via environment variables:
  REMOTE_HOST     Required. IP or hostname of the Raspberry Pi.
  REMOTE_USER     SSH username (default: pi)
  REMOTE_DIR      temp dir on remote for binaries (default: /tmp/quicmq-bench)
  REMOTE_ARCH     Go GOARCH for cross-compile (default: arm64)
  REMOTE_OS       Go GOOS for cross-compile (default: linux)
  LOCAL_IP        local IP reachable from Pi (auto-detected if unset)
  LOCAL_PUBS      publishers to start on this machine (default: 2)
  LOCAL_SUBS      subscribers to start on this machine (default: 5)
  REMOTE_PUBS     publishers to start on Pi (default: 2)
  REMOTE_SUBS     subscribers to start on Pi (default: 5)
  TOPIC           pub/sub topic prefix (default: data)
  MSG_RATE        total messages/s across all publishers (default: 500)
  MSG_SIZE        bytes per message (default: 256)
  DURATION        run seconds (default: 30)
  MODE            pubsub | reqrep | datagram (default: pubsub)
  SCENARIO        label embedded in result JSON (default: phys)
  RESULTS_DIR     local dir to write results (set by run.sh)
"""

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

# ── Configuration ─────────────────────────────────────────────────────────────

def _env(key, default=""):
    return os.environ.get(key, default)

def _env_int(key, default):
    try:
        return int(os.environ.get(key, default))
    except (ValueError, TypeError):
        return default

REMOTE_HOST = _env("REMOTE_HOST", "")
REMOTE_USER = _env("REMOTE_USER", "pi")
REMOTE_DIR  = _env("REMOTE_DIR", "/tmp/quicmq-bench")
REMOTE_ARCH = _env("REMOTE_ARCH", "amd64")
REMOTE_OS   = _env("REMOTE_OS", "linux")
TRANSPORT   = _env("TRANSPORT", "quic")   # quic | tcp

LOCAL_IP    = _env("LOCAL_IP", "")
LOCAL_PUBS  = _env_int("LOCAL_PUBS", 2)
LOCAL_SUBS  = _env_int("LOCAL_SUBS", 5)
REMOTE_PUBS = _env_int("REMOTE_PUBS", 2)
REMOTE_SUBS = _env_int("REMOTE_SUBS", 5)

TOPIC       = _env("TOPIC", "data")
MSG_RATE    = _env_int("MSG_RATE", 500)
MSG_SIZE    = _env_int("MSG_SIZE", 256)
DURATION    = _env_int("DURATION", 30)
CONCURRENCY = _env_int("CONCURRENCY", 5)
MODE        = _env("MODE", "pubsub")
SCENARIO    = _env("SCENARIO", "phys")

SCRIPT_DIR  = Path(__file__).parent
REPO_ROOT   = SCRIPT_DIR.parent.parent.parent   # phys/ -> scenarios/ -> benchmarks/ -> repo root
BIN_SRC     = REPO_ROOT / "benchmarks" / "scenarios" / "cmd"
LOCAL_BINS  = REPO_ROOT / "benchmarks" / "scenarios" / "_bin"
REMOTE_BINS = LOCAL_BINS / "remote"

_rd = _env("RESULTS_DIR", "")
RESULTS_DIR = Path(_rd) if _rd else SCRIPT_DIR / "results" / SCENARIO

REMOTE_SSH = f"{REMOTE_USER}@{REMOTE_HOST}"

# PUB/DPUB base ports.  Multiple publishers use consecutive ports.
PUB_BASE_PORT  = 9900
DPUB_BASE_PORT = 9700
REP_PORT       = 9800

# ── Logging ───────────────────────────────────────────────────────────────────

def log(msg):
    print(f"[phys] {msg}", flush=True)

def warn(msg):
    print(f"[phys] WARN: {msg}", flush=True)

def die(msg):
    print(f"[phys] FATAL: {msg}", file=sys.stderr)
    sys.exit(1)

# ── Network helpers ────────────────────────────────────────────────────────────

def detect_local_ip():
    if LOCAL_IP:
        return LOCAL_IP
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect((REMOTE_HOST, 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return socket.gethostbyname(socket.gethostname())

def resolve_remote_ip():
    try:
        return socket.gethostbyname(REMOTE_HOST)
    except socket.gaierror:
        return REMOTE_HOST   # use as-is if DNS fails

# ── Build ──────────────────────────────────────────────────────────────────────

def build_binary(role, goarch, goos, out_path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    log(f"building {role:<6} GOOS={goos} GOARCH={goarch} → {out_path.relative_to(REPO_ROOT)}")
    env = os.environ.copy()
    env["GOOS"] = goos
    env["GOARCH"] = goarch
    if goarch == "arm":
        env.setdefault("GOARM", "7")
    r = subprocess.run(
        ["go", "build", "-o", str(out_path), f"./benchmarks/scenarios/cmd/{role}"],
        cwd=str(REPO_ROOT),
        capture_output=True, text=True, env=env,
    )
    if r.returncode != 0:
        die(f"build failed for {role} ({goos}/{goarch}):\n{r.stderr}")

def build_all(roles):
    import platform
    local_arch = "amd64"
    local_goos = "linux"
    # Handle macOS dev machines cross-compiling to linux.
    if platform.system() == "Darwin":
        local_goos = "darwin"
        local_arch = "arm64" if platform.machine() == "arm64" else "amd64"

    for role in roles:
        build_binary(role, local_arch, local_goos, LOCAL_BINS / role)
        build_binary(role, REMOTE_ARCH, REMOTE_OS, REMOTE_BINS / role)

# ── SSH / SCP ─────────────────────────────────────────────────────────────────

_SSH_OPTS = ["-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes"]

def ssh_run(cmd, check=True):
    r = subprocess.run(["ssh"] + _SSH_OPTS + [REMOTE_SSH, cmd],
                       capture_output=True, text=True)
    if check and r.returncode != 0:
        die(f"SSH command failed: {cmd}\n{r.stderr}")
    return r.stdout

def scp_to(local_path, remote_path):
    subprocess.run(
        ["scp"] + _SSH_OPTS + [str(local_path), f"{REMOTE_SSH}:{remote_path}"],
        check=True, capture_output=True,
    )

def deploy_binaries(roles):
    log(f"deploying binaries to {REMOTE_SSH}:{REMOTE_DIR}/")
    ssh_run(f"mkdir -p {REMOTE_DIR}")
    for role in roles:
        remote_path = f"{REMOTE_DIR}/{role}"
        scp_to(REMOTE_BINS / role, remote_path)
        ssh_run(f"chmod +x {remote_path}")
    log(f"deployed: {', '.join(roles)}")

# ── Process wrappers ───────────────────────────────────────────────────────────

class Proc:
    """Thin wrapper around a subprocess (local or SSH)."""

    def __init__(self, role, env_vars, argv):
        self.role = role
        self.output = ""
        self._env_vars = env_vars  # dict of extra env vars
        self._argv = argv          # list[str] command

    def start(self):
        full_env = os.environ.copy()
        full_env.update(self._env_vars)
        self._proc = subprocess.Popen(
            self._argv,
            env=full_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    def wait(self, timeout=15):
        try:
            stdout, _ = self._proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            self._proc.kill()
            stdout, _ = self._proc.communicate()
        self.output = stdout.strip()

    def terminate(self):
        try:
            if self._proc.poll() is None:
                self._proc.terminate()
        except Exception:
            pass


def local_proc(role, env_vars):
    return Proc(role, env_vars, [str(LOCAL_BINS / role)])


def remote_proc(role, env_vars):
    """Run a binary on the remote host via SSH, capturing stdout."""
    remote_bin = f"{REMOTE_DIR}/{role}"
    env_prefix = " ".join(f"{k}={v}" for k, v in env_vars.items())
    cmd = f"{env_prefix} {remote_bin}"
    return Proc(role, {}, ["ssh"] + _SSH_OPTS + [REMOTE_SSH, cmd])

# ── Scenario runners ───────────────────────────────────────────────────────────

def run_pubsub(local_ip, remote_ip):
    rate_per_pub = max(1, MSG_RATE // max(1, LOCAL_PUBS + REMOTE_PUBS))

    # All publisher addresses: local ones first, then remote.
    all_addrs = (
        [f"{TRANSPORT}://{local_ip}:{PUB_BASE_PORT + i}" for i in range(LOCAL_PUBS)] +
        [f"{TRANSPORT}://{remote_ip}:{PUB_BASE_PORT + i}" for i in range(REMOTE_PUBS)]
    )

    procs = []

    # Publishers — local.
    for i in range(LOCAL_PUBS):
        procs.append(local_proc("pub", {
            "LISTEN_ADDR": f"{TRANSPORT}://0.0.0.0:{PUB_BASE_PORT + i}",
            "TOPIC": TOPIC, "MSG_RATE": str(rate_per_pub),
            "MSG_SIZE": str(MSG_SIZE), "DURATION": str(DURATION + 5),
            "SCENARIO": SCENARIO, "TRANSPORT": TRANSPORT,
        }))

    # Publishers — remote.
    for i in range(REMOTE_PUBS):
        procs.append(remote_proc("pub", {
            "LISTEN_ADDR": f"{TRANSPORT}://0.0.0.0:{PUB_BASE_PORT + i}",
            "TOPIC": TOPIC, "MSG_RATE": str(rate_per_pub),
            "MSG_SIZE": str(MSG_SIZE), "DURATION": str(DURATION + 5),
            "SCENARIO": SCENARIO, "TRANSPORT": TRANSPORT,
        }))

    for p in procs:
        p.start()

    log(f"publishers started ({LOCAL_PUBS} local, {REMOTE_PUBS} remote) — waiting 3s")
    time.sleep(3)

    # Subscribers — each connects to one pub address, round-robin across all pubs.
    sub_procs = []

    for i in range(LOCAL_SUBS):
        addr = all_addrs[i % len(all_addrs)]
        sub_procs.append(local_proc("sub", {
            "SERVER_ADDR": addr,
            "TOPIC": TOPIC, "DURATION": str(DURATION),
            "SCENARIO": SCENARIO, "TRANSPORT": TRANSPORT,
        }))

    for i in range(REMOTE_SUBS):
        addr = all_addrs[i % len(all_addrs)]
        sub_procs.append(remote_proc("sub", {
            "SERVER_ADDR": addr,
            "TOPIC": TOPIC, "DURATION": str(DURATION),
            "SCENARIO": SCENARIO, "TRANSPORT": TRANSPORT,
        }))

    for p in sub_procs:
        p.start()

    log(f"subscribers started ({LOCAL_SUBS} local, {REMOTE_SUBS} remote)")
    log(f"running for {DURATION}s…")
    time.sleep(DURATION + 6)

    procs.extend(sub_procs)
    return _collect(procs)


def run_reqrep(local_ip, remote_ip):
    """REP server on local machine; REQ clients on remote."""
    rep = local_proc("rep", {
        "LISTEN_ADDR": f"{TRANSPORT}://0.0.0.0:{REP_PORT}",
        "DURATION": str(DURATION + 5), "SCENARIO": SCENARIO, "TRANSPORT": TRANSPORT,
    })
    rep.start()

    time.sleep(2)

    req = remote_proc("req", {
        "SERVER_ADDR": f"{TRANSPORT}://{local_ip}:{REP_PORT}",
        "CONCURRENCY": str(CONCURRENCY),
        "MSG_SIZE": str(MSG_SIZE), "DURATION": str(DURATION),
        "SCENARIO": SCENARIO, "TRANSPORT": TRANSPORT,
    })
    req.start()

    log(f"REP on local, {CONCURRENCY} REQ workers on remote — running for {DURATION}s…")
    time.sleep(DURATION + 6)

    return _collect([rep, req])


def run_datagram(local_ip, remote_ip):
    """Datagram publisher on local; datagram subscribers on remote (QUIC-only)."""
    dpub = local_proc("dpub", {
        "LISTEN_ADDR": f"quic://0.0.0.0:{DPUB_BASE_PORT}",
        "TOPIC": TOPIC, "MSG_RATE": str(MSG_RATE),
        "MSG_SIZE": str(MSG_SIZE), "DURATION": str(DURATION + 5),
        "SCENARIO": SCENARIO,
    })
    dpub.start()
    time.sleep(2)

    dsubs = []
    for i in range(REMOTE_SUBS):
        dsubs.append(remote_proc("dsub", {
            "SERVER_ADDR": f"quic://{local_ip}:{DPUB_BASE_PORT}",
            "TOPIC": TOPIC, "DURATION": str(DURATION),
            "SCENARIO": SCENARIO,
        }))

    for p in dsubs:
        p.start()

    log(f"datagram pub on local, {REMOTE_SUBS} dsub on remote — running for {DURATION}s…")
    time.sleep(DURATION + 6)

    return _collect([dpub] + dsubs)


def _collect(procs):
    outputs = []
    for p in procs:
        p.wait(timeout=15)
        p.terminate()
        if p.output:
            outputs.append((p.role, p.output))
    return outputs

# ── Result saving / summary ────────────────────────────────────────────────────

def save_outputs(outputs):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    counts = {}
    for role, text in outputs:
        n = counts.get(role, 0)
        counts[role] = n + 1
        path = RESULTS_DIR / f"{role}-{n}.jsonl"
        path.write_text(text + "\n")
        log(f"saved {path}")


def print_summary(outputs):
    total_pubs = LOCAL_PUBS + REMOTE_PUBS
    total_subs = LOCAL_SUBS + REMOTE_SUBS
    print(f"\n{'━'*62}")
    print(f"  Scenario : {SCENARIO}   mode: {MODE}")
    print(f"  Local    : {LOCAL_PUBS} pub(s) + {LOCAL_SUBS} sub(s)  [{detect_local_ip()}]")
    print(f"  Remote   : {REMOTE_PUBS} pub(s) + {REMOTE_SUBS} sub(s)  [{REMOTE_HOST}]")
    print(f"  Total    : {total_pubs} publisher(s)  {total_subs} subscriber(s)")
    print(f"{'━'*62}")
    for role, text in outputs:
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
                print(f"  {r:<6}  sent={obj.get('msgs_sent', 0):<8}  "
                      f"rate={obj.get('actual_rate', 0):<8.0f} msg/s  "
                      f"{obj.get('throughput_mbs', 0):.2f} MB/s")
            elif r in ("sub", "dsub"):
                print(f"  {r:<6}  rcvd={obj.get('msgs_received', 0):<8}  "
                      f"gaps={obj.get('seq_gaps', 0):<5}  "
                      f"p50={obj.get('latency_p50_ms', 0):.2f}ms  "
                      f"p99={obj.get('latency_p99_ms', 0):.2f}ms")
            elif r == "rep":
                print(f"  rep     handled={obj.get('reqs_handled', 0):<8}  "
                      f"rate={obj.get('actual_rate', 0):.0f} req/s")
            elif r == "req":
                print(f"  req     sent={obj.get('reqs_sent', 0):<8}  "
                      f"p50={obj.get('rtt_p50_ms', 0):.2f}ms  "
                      f"p99={obj.get('rtt_p99_ms', 0):.2f}ms  "
                      f"err={obj.get('errors', 0)}")
    print()

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    if not REMOTE_HOST:
        die(
            "REMOTE_HOST is required.\n"
            "Example: REMOTE_HOST=192.168.1.42 REMOTE_USER=pi ./run.sh phys pubsub_baseline"
        )

    local_ip  = detect_local_ip()
    remote_ip = resolve_remote_ip()
    log(f"local={local_ip}  remote={REMOTE_HOST} ({remote_ip})  arch={REMOTE_ARCH}  transport={TRANSPORT}")

    mode_roles = {
        "pubsub":   ["pub", "sub"],
        "reqrep":   ["rep", "req"],
        "datagram": ["dpub", "dsub"],
    }
    roles = mode_roles.get(MODE)
    if roles is None:
        die(f"unknown MODE={MODE!r}  (pubsub|reqrep|datagram)")

    log("building binaries…")
    build_all(roles)

    deploy_binaries(roles)

    log(f"starting scenario '{SCENARIO}'  mode={MODE}  duration={DURATION}s")
    if MODE == "pubsub":
        outputs = run_pubsub(local_ip, remote_ip)
    elif MODE == "reqrep":
        outputs = run_reqrep(local_ip, remote_ip)
    else:
        outputs = run_datagram(local_ip, remote_ip)

    save_outputs(outputs)
    print_summary(outputs)
    log(f"results in {RESULTS_DIR}/")


if __name__ == "__main__":
    main()
