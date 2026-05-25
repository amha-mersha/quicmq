# QuicMQ Benchmark Scenario Runner — Usage Guide

## Overview

`run.sh` orchestrates multi-node QuicMQ benchmark scenarios in two modes:

| Mode   | Backend     | Purpose                                         |
|--------|-------------|-------------------------------------------------|
| `dev`  | Docker      | Fast iteration on a single machine, CI-friendly |
| `prod` | Mininet     | Realistic multi-host network simulation for thesis data |

Results are written as JSON to `results/<scenario>/` (dev) or `prod/results/<scenario>/` (prod).

---

## Quick Start

```bash
# List all available scenarios
./run.sh dev list

# Run a single dev scenario (Docker required)
./run.sh dev pubsub_baseline

# Run all dev scenarios
./run.sh dev all

# Run a prod scenario (mininet + sudo required)
./run.sh prod prod_pubsub_baseline

# Full help
./run.sh --help
```

---

## Dev Mode (Docker)

Dev mode uses `docker compose` with `tc-netem` inside the client containers.
The publisher/server containers are pristine; only client-side containers have
network degradation applied.

### Requirements

- Docker with Compose v2 (`docker compose version`)
- `jq` for result parsing

### Scenario families

| Family                | Description                                           |
|-----------------------|-------------------------------------------------------|
| `pubsub_baseline`     | Clean network, 3 subscribers, 1000 msg/s              |
| `pubsub_fanout_stress`| 10 subscribers — per-stream flow control independence |
| `pubsub_highrate`     | 5000 msg/s ceiling test                               |
| `pubsub_largemsg`     | 8 KiB messages — QUIC stream framing overhead         |
| `pubsub_loss_5pct`    | 5% packet loss — QUIC transparent recovery            |
| `pubsub_loss_20pct`   | 20% loss — retransmission cost                        |
| `pubsub_latency_50ms` | 50 ms RTT — intercontinental link simulation          |
| `pubsub_latency_200ms`| 200 ms RTT — satellite link                           |
| `pubsub_bandwidth_1mbit`| 1 Mbit/s cap — flow control behaviour               |
| `pubsub_lossy_latency`| 5% loss + 50 ms + 5 ms jitter — mobile network       |
| `reqrep_baseline`     | REQ/REP, 5 concurrent clients                         |
| `reqrep_stress`       | 20 concurrent clients                                 |
| `reqrep_multinode_stress`| 5 containers × 5 goroutines = 25 concurrent      |
| `reqrep_loss_10pct`   | 10% loss — RTT tail latency growth                    |
| `reqrep_latency_50ms` | 50 ms one-way delay                                   |
| `reqrep_latency_100ms`| 100 ms one-way delay                                  |
| `reqrep_lossy_latency`| 10% loss + 50 ms delay                               |
| `reqrep_reorder`      | 10% packet reordering                                 |
| `datagram_baseline`   | RFC 9221 datagram PUB/SUB baseline                    |
| `datagram_highrate`   | High-rate datagrams, no retransmit overhead           |
| `datagram_loss_5pct`  | 5% loss — visible as seq_gaps (no QUIC hiding)        |
| `datagram_loss_20pct` | 20% loss — reliability trade-off                      |
| `datagram_vs_stream`  | Side-by-side: stream vs datagram under 5% loss        |
| `datagram_latency`    | 50 ms delay — datagram vs stream HOL blocking         |

### Running specific scenarios

```bash
./run.sh dev pubsub_loss_5pct reqrep_latency_50ms datagram_baseline
```

### Building the Docker image only

```bash
./run.sh dev build
```

---

## Prod Mode (Mininet)

Prod mode creates a two-host virtual network using Linux network namespaces via
mininet.  **This mode requires `sudo`** because mininet manipulates kernel
network state.

```
h1 (pub/rep server)  ──[ configurable link ]──  h2 (sub/req clients)
```

Link parameters (delay, loss, bandwidth) are applied at the kernel level, making
the simulation more accurate than Docker's container-level netem.

### Requirements

```bash
sudo apt-get install -y mininet   # Ubuntu/Debian
```

Python package: `pip install mininet` (alternative)

Run checks:

```bash
sudo mn --version
python3 -c "import mininet; print('ok')"
```

### Running prod scenarios

```bash
# Single scenario
./run.sh prod prod_pubsub_baseline

# Multiple
./run.sh prod prod_pubsub_loss_5pct prod_reqrep_latency_50ms

# All prod scenarios
./run.sh prod all
```

### Prod scenario catalogue

| Scenario                    | Description                                   |
|-----------------------------|-----------------------------------------------|
| `prod_pubsub_baseline`      | 1 pub + 3 subs, clean link                    |
| `prod_pubsub_fanout`        | 1 pub + 10 subs                               |
| `prod_pubsub_loss_5pct`     | 5% packet loss on link                        |
| `prod_pubsub_loss_20pct`    | 20% packet loss on link                       |
| `prod_pubsub_latency_50ms`  | 50 ms one-way delay                           |
| `prod_pubsub_latency_200ms` | 200 ms one-way delay                          |
| `prod_pubsub_bandwidth_1mbit`| 1 Mbit/s bandwidth cap                       |
| `prod_pubsub_multinode`     | 10 pubs + 30 subs — thesis "prod" setup       |
| `prod_reqrep_baseline`      | 5 concurrent REQ workers                      |
| `prod_reqrep_stress`        | 25 concurrent REQ workers                     |
| `prod_reqrep_latency_50ms`  | 50 ms delay                                   |
| `prod_reqrep_loss_10pct`    | 10% loss                                      |
| `prod_datagram_baseline`    | RFC 9221 datagram, clean link                 |
| `prod_datagram_loss_5pct`   | 5% loss — visible seq_gaps                    |
| `prod_datagram_loss_20pct`  | 20% loss — reliability trade-off              |

---

## Metrics Explained

| Field              | Unit    | Meaning                                              |
|--------------------|---------|------------------------------------------------------|
| `msgs_sent`        | count   | Total messages published                             |
| `msgs_received`    | count   | Total messages received by subscriber                |
| `seq_gaps`         | count   | Sequence number gaps (dropped/reordered messages)    |
| `actual_rate`      | msg/s   | Measured throughput (not configured target)          |
| `throughput_mbs`   | MB/s    | Effective data throughput including framing          |
| `latency_p50_ms`   | ms      | 50th-percentile one-way latency                      |
| `latency_p99_ms`   | ms      | 99th-percentile one-way latency                      |
| `rtt_p50_ms`       | ms      | 50th-percentile round-trip time (REQ/REP)            |
| `rtt_p99_ms`       | ms      | 99th-percentile round-trip time (REQ/REP)            |
| `errors`           | count   | Send/receive errors (non-fatal)                      |

Latency is measured **including transport + encryption overhead** (TLS for QUIC,
CURVE for TCP).  See [Post-Handshake Timing](#post-handshake-timing) below for
isolating the data-transfer phase.

---

## Post-Handshake Timing

QUIC uses TLS 1.3 and TCP uses ZMTP CURVE — the handshake costs differ, making
a raw first-message latency comparison unfair.  To measure only the data-transfer
phase:

1. Enable qlog on the QUIC side by setting `QLOGDIR=./qlogs` before running
   any scenario binary, or by using `quicmq.WithQlogDir()` in the publisher.

2. After a run, parse the `.sqlog` files to extract the timestamp of the first
   `packet_received` event with a `STREAM` frame (i.e., the first application
   data packet after handshake).

3. Compare that timestamp against the send-side timestamp embedded in the
   message payload (`<topic>|<seq>|<send_ns>|…`).

The difference is the **post-handshake one-way latency** — directly comparable
between QUIC and TCP once the connection is established.

### Enabling qlog

```bash
# Dev mode — set env var before running the scenario binary directly
QLOGDIR=./qlogs go run ./benchmarks/scenarios/cmd/pub

# Or pass the option in Go code:
pub := quicmq.NewPub(ctx, quicmq.WithQlogDir("./qlogs"))
```

Qlog files are written to `<QLOGDIR>/<connection-id>_server.sqlog` (publisher)
and `<connection-id>_client.sqlog` (subscriber).  Each is a newline-delimited
JSON stream following [draft-ietf-quic-qlog-main-schema].

### Parsing qlogs for handshake boundary

```bash
# Find the first STREAM frame timestamp (post-handshake data)
jq 'select(.name == "transport:packet_received")
    | select(.data.frames[]?.frame_type == "stream")
    | .time' ./qlogs/*.sqlog | head -1
```

---

## Network Simulation Parameters

Both dev and prod modes accept these environment variables:

| Variable             | Default | Description                        |
|----------------------|---------|------------------------------------|
| `NETEM_DELAY_MS`     | `0`     | One-way delay in milliseconds      |
| `NETEM_JITTER_MS`    | `0`     | Delay jitter in milliseconds       |
| `NETEM_LOSS_PCT`     | `0`     | Packet loss percentage             |
| `NETEM_RATE_KBIT`    | `0`     | Bandwidth cap in kbit/s (0=∞)     |
| `NETEM_REORDER_PCT`  | `0`     | Packet reorder percentage          |
| `NETEM_CORRUPT_PCT`  | `0`     | Bit corruption percentage (dev only)|

In dev mode these are applied to client containers via `tc-netem`.
In prod mode they are applied to the mininet link between h1 and h2.

RTT = 2 × `NETEM_DELAY_MS` (delay is one-way).

---

## Results Directory Layout

```
benchmarks/scenarios/results/
└── pubsub_baseline/
    ├── pub.jsonl       ← publisher JSON result (one line per replica)
    ├── sub.jsonl       ← subscriber JSON results
    └── req.jsonl       ← requester JSON results (req/rep scenarios)

benchmarks/scenarios/prod/results/
└── prod_pubsub_baseline/
    ├── pub-0.jsonl
    └── sub-0.jsonl … sub-2.jsonl
```

Each `.jsonl` file contains one JSON object per container/process replica.

---

## Adding a Custom Scenario

In `run.sh`, add a function following this pattern:

```bash
scenario_my_custom_test() {
    reset_net
    export TOPIC=mydata MSG_RATE=2000 MSG_SIZE=512 DURATION=60
    export NETEM_DELAY_MS=30 NETEM_LOSS_PCT=1
    run_scenario "my_custom_test" "pub sub" "--scale sub=5"
}
```

Then add `my_custom_test` to the `ALL_SCENARIOS` array and call:

```bash
./run.sh dev my_custom_test
```

For prod, follow the same pattern with `prod_run_scenario` and add to
`ALL_PROD_SCENARIOS`.
