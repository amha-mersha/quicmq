# CLAUDE.md — ZMQ over QUIC (quicmq) Thesis Project

> **Read this file in full before writing any code, tests, docs, or thesis content.**
> This file is your single source of truth for the entire project.

---

## 0. Project Overview

You are building **`zmqquic`** — a Go library that implements the ZeroMQ messaging
patterns (initially **PUB/SUB** and **REQ/REP**) on top of the QUIC transport
protocol.

This is a **thesis project**. Every feature you add must be accompanied by:

1. Updated Go implementation code
2. Updated unit/integration tests
3. Updated benchmarks (with recorded results)(run as needed when feature implementation if done.)
4. Updated LaTeX thesis (`thesis/main.tex`)
5. Updated GitHub Pages documentation (`docs/`)

Work in **small, complete increments**: implement → test → benchmark → document → update thesis.

---

## 1. Repository Layout

```
zmqquic/
├── CLAUDE.md                  ← this file
├── go.mod
├── go.sum
├── README.md
│
├── examples/                   ← different examples for both pubsub and reqrep patterns showing different aspects of the project.
│   ├── pubsub/
│   │   ├── publisher/main.go
│   │   └── subscriber/main.go
│   └── reqrep/
│       ├── client/main.go
│       └── server/main.go
│
├── benchmarks/
│   ├── bench_tests.go         ← multiple of them, testing different things (they could be single or multiple files for a test, a .go or a .sh file based on the need.)
│   └── results/               ← JSON + CSV results saved here
│       └── .gitkeep
│
├── thesis/
│   ├── main.tex               ← master LaTeX file
│   ├── sections/              ← the sections based on thesis_paper_instruction.pdf
│   ├── figures/               ← diagrams, benchmark plots
│   └── thesis_paper_instruction.pdf
│
└── docs/                      ← GitHub Pages site (Jekyll or plain HTML)
    ├── index.md
    ├── getting-started.md
    ├── api-reference.md
    ├── patterns/
    │   ├── pubsub.md
    │   └── reqrep.md
    └── _config.yml            ← Jekyll config
```

---

## 2. Reference Sources — HOW to use each

### 2.1 libzmq (C reference implementation)

- **Repo**: `https://github.com/zeromq/libzmq` or locally at `../libzmq`
- **Purpose**: Understand the _canonical_ behaviour of each socket type, the
  different ZMTP mechanism (framing logic, subscription filtering, and the ready/ping/pong).
- **Do NOT copy C code**; use it only to understand semantics and edge-cases.

### 2.2 zmq4 (Go reference for ZMTP protocol)

- **Repo**: `https://github.com/go-zeromq/zmq4` or locally at `../zmq4`
- **Purpose**: This is the idiomatic Go implementation of ZMTP 3.x over TCP.
  Use it to see how to structure (but is incomplete):

### 2.3 quic-go

- **Package**: `github.com/quic-go/quic-go`
- **Go doc** (local): run `go doc github.com/quic-go/quic-go` after `go get`
- **Online**: `https://pkg.go.dev/github.com/quic-go/quic-go` `https://quic-go.net/docs/quic`
- **Mapping to ZMQ patterns**:
  - REQ/REP → one QUIC bidirectional stream per request/response exchange
  - PUB/SUB → publisher opens one unidirectional stream _per subscriber_;
    OR use QUIC datagrams (if unreliable delivery is acceptable)
- Read `quic.Config.EnableDatagrams` for PUB/SUB unreliable variant.

### 2.4 RFCs — what to check in each

| RFC                                                                                                    | When to consult                                                              |
| ------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------- |
| [RFC 9000](https://datatracker.ietf.org/doc/html/rfc9000)                                              | Core QUIC transport — streams, flow control, connection IDs, migration       |
| [RFC 9001](https://datatracker.ietf.org/doc/html/rfc9001)                                              | QUIC-TLS integration — how TLS 1.3 handshake maps to QUIC packets            |
| [RFC 9002](https://datatracker.ietf.org/doc/html/rfc9002)                                              | QUIC loss detection & congestion control — relevant for benchmark analysis   |
| [RFC 9221](https://datatracker.ietf.org/doc/html/rfc9221)                                              | QUIC Unreliable Datagrams — use for PUB/SUB best-effort variant              |
| [RFC 8899](https://datatracker.ietf.org/doc/rfc8899/)                                                  | Packetization Layer Path MTU Discovery — relevant if you tune datagram sizes |
| [RFC 9369](https://datatracker.ietf.org/doc/html/rfc9369)                                              | QUIC v2 — note differences from v1; quic-go supports it                      |
| [draft-ietf-quic-qlog-main-schema](https://datatracker.ietf.org/doc/draft-ietf-quic-qlog-main-schema/) | qlog schema for structured QUIC event logging                                |
| [draft-ietf-quic-qlog-quic-events](https://datatracker.ietf.org/doc/draft-ietf-quic-qlog-quic-events/) | Specific QUIC events in qlog format — use for debugging & thesis figures     |

**Cite RFCs in the thesis** using `\cite{rfc9000}` etc. Add them to `references.bib`.

---

## 3. ZMTP Wire Format (what you must implement)

### 3.1 Greeting (64 bytes)

```
Byte  0      : 0xFF  (magic)
Bytes 1-8    : 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x01  (padding + revision)
Byte  9      : 0x7F  (magic)
Byte  10     : 0x03  (major version = 3)
Byte  11     : 0x01  (minor version = 1)
Bytes 12-31  : mechanism, e.g. "NULL" + padding
Byte  32     : as-server flag (0 or 1)
Bytes 33-63  : filler (0x00)
```

### 3.2 Frames

```
Short frame (body ≤ 255 bytes):
  flags (1 byte) | body-length (1 byte) | body

Long frame (body > 255 bytes):
  flags (1 byte) | 0x02 set in flags | body-length (8 bytes BE) | body

Flags bits:
  bit 0 = MORE  (more message parts follow)
  bit 1 = LONG  (8-byte length)
  bit 2 = CMD   (command frame, not message)
```

### 3.3 Commands

```
READY   : \x05READY  + metadata properties
SUBSCRIBE / CANCEL : used by SUB socket
PING / PONG : keepalive
ERROR   : \x05ERROR + reason
```

### 3.4 Metadata properties (in READY command)

```
property = name-length (1 byte) | name | value-length (4 bytes BE) | value
Required: "Socket-Type" → "PUB" | "SUB" | "REQ" | "REP"
Optional: "Identity", "Resource"
```

---

## 4. Socket Semantics to Implement

### PUB / SUB

- PUB sends to **all** connected SUBs (fan-out).
- SUB sends a SUBSCRIBE command with a prefix byte `\x01` + topic bytes.
- SUB sends CANCEL with prefix byte `\x00` + topic bytes.
- PUB **filters** outgoing messages: only sends if message[0] starts with
  any subscribed prefix (empty prefix = receive all).
- Messages are multi-part; filtering applies to part[0] only.
- PUB drops messages if subscriber is slow (HWM / drop policy).

### REQ / REP

- Strict alternation: REQ must send before it can receive; REP must receive
  before it can send.
- REQ automatically wraps outgoing messages with an empty delimiter frame.
- REP automatically strips the delimiter on receive and re-adds on send.
- Implement with a `turn` mutex / channel: block the wrong-direction call.
- One QUIC bidirectional stream per request-response cycle (open new stream
  for each REQ send, close after REP send, or keep a pool).

---

## 5. QUIC Transport Design Decisions (document these in the thesis)

| Decision         | Options                                       | Recommended                                            |
| ---------------- | --------------------------------------------- | ------------------------------------------------------ |
| PUB→SUB delivery | One stream per subscriber vs. datagrams       | Start with streams; add datagram variant as extension  |
| REQ/REP streams  | One stream per RPC vs. mux over one stream    | One stream per RPC (simpler; QUIC handles concurrency) |
| TLS              | Self-signed cert (InsecureSkipVerify) for dev | Expose TLS config; default to self-signed for tests    |
| Flow control     | Default quic-go defaults                      | Expose as socket options                               |
| Keep-alive       | QUIC PING frames (built-in)                   | Enable via `quic.Config.KeepAlivePeriod`               |
| Addressing       | `quic://host:port` scheme                     | Parse in `Bind()`/`Connect()`                          |

---

## 6. Test Plan

### 6.1 Unit Tests (`zmqquic/*_test.go`)

- Frame encode/decode roundtrip (all flag combinations)
- Greeting encode/decode
- READY command encode/decode
- Subscription filter logic (prefix match)
- REQ/REP turn enforcement (send-before-recv, recv-before-send errors)

### 6.2 Integration Tests (`tests/integration/`)

- PUB/SUB: single publisher, single subscriber, topic filter works
- PUB/SUB: single publisher, 3 subscribers, fan-out
- PUB/SUB: subscriber with empty topic receives all messages
- PUB/SUB: subscriber with non-matching topic receives nothing
- REQ/REP: single round-trip
- REQ/REP: 100 sequential round-trips
- REQ/REP: concurrent REQ sockets to one REP
- Error cases: connect before bind, close mid-transfer

### 6.3 Conformance Tests (`tests/conformance/`)

- Wire-level: use `net.Dial` / raw bytes to send a valid ZMTP greeting and
  READY command to a `zmqquic` REP server; verify it responds correctly.
- Interop check: if time allows, test against `zmq4` TCP sockets via a bridge.

### 6.4 Benchmarks

Benchmarks are going to be run agains a tcp based zmq library, you'll implement a tcp based ZMTP in here (by adding a tcp transport layer) and compare the two.

---

## 7. Thesis Structure & Update Protocol

### LaTeX file: `thesis/main.tex`

Use the template format at `./thesis/thesis_paper_instruction.pdf`

---

## 8. GitHub Pages Documentation (`docs/`)

Use Jekyll with the `just-the-docs` theme.

### `docs/_config.yml`

```yaml
title: zmqquic
description: ZeroMQ messaging patterns over QUIC
theme: just-the-docs
remote_theme: just-the-docs/just-the-docs
url: https://<username>.github.io/zmqquic
color_scheme: dark
search_enabled: true
nav_sort: case_insensitive
```

### Pages to create/maintain

| File                      | Content                                                                |
| ------------------------- | ---------------------------------------------------------------------- |
| `docs/index.md`           | Hero intro, install (`go get`), 10-line quick-start                    |
| `docs/getting-started.md` | Installation, TLS config, first PUB/SUB example, first REQ/REP example |
| `docs/api-reference.md`   | All public types, interfaces, functions with descriptions              |
| `docs/patterns/pubsub.md` | PUB/SUB pattern deep-dive, topic filtering, fan-out, options           |
| `docs/patterns/reqrep.md` | REQ/REP pattern deep-dive, error handling, concurrency                 |
| `docs/internals.md`       | QUIC stream mapping, ZMTP framing, architecture diagram                |
| `docs/benchmarks.md`      | Latest benchmark results (auto-updated)                                |
| `docs/contributing.md`    | How to run tests, benchmark, build thesis                              |

Every doc page must have working Go code examples that compile against the library.

---

## 9. Development Workflow for Claude Code

Follow this exact sequence for each feature:

```
1. UNDERSTAND
   - Read relevant libzmq source (browse GitHub)
   - Read zmq4 equivalent Go code
   - Read relevant RFC sections
   - Read quic-go docs for the QUIC primitives needed

2. IMPLEMENT
   - Write the Go code in zmqquic/
   - Keep public API consistent with §6

3. TEST
   - Write unit tests first (TDD where possible)
   - Write integration tests
   - Run: go test ./... -race -count=1

4. BENCHMARK (if applicable)

5. UPDATE THESIS
   - Update the relevant chapter(s) in thesis/chapters/
   - Add/update figures in thesis/figures/
   - Ensure references.bib is up to date
   - Verify it compiles: pdflatex thesis/main.tex

6. UPDATE DOCS
   - Update relevant docs/ pages
   - Keep code examples up to date

7. COMMIT MESSAGE format:
   feat(pub): implement topic prefix filter + update thesis ch04 + docs
```

---

## 10. Implementation Order (suggested milestones)

### Milestone 1 — Foundation

- [ ] `go.mod`, project scaffold
- [ ] ZMTP frame encoder/decoder (`frame.go`)
- [ ] ZMTP greeting (`greeting.go`)
- [ ] QUIC transport wrapper (`internal/quicconn/conn.go`)
- [ ] Basic `Socket` interface + `Context`
- [ ] Thesis: chapters 1–3 skeleton, references.bib

### Milestone 2 — REQ/REP

- [ ] `req.go`, `rep.go` with turn enforcement
- [ ] Integration tests for REQ/REP
- [ ] `examples/reqrep/`
- [ ] Benchmarks: latency + throughput
- [ ] Thesis: chapter 4 (REQ/REP section), chapter 5 (REQ/REP benchmarks)
- [ ] Docs: getting-started, reqrep pattern page

### Milestone 3 — PUB/SUB

- [ ] `pub.go`, `sub.go` with prefix filter
- [ ] Integration tests for PUB/SUB (single + fan-out)
- [ ] `examples/pubsub/`
- [ ] Benchmarks: throughput + fan-out + filter
- [ ] Thesis: chapter 4 (PUB/SUB section), chapter 5 (PUB/SUB benchmarks)
- [ ] Docs: pubsub pattern page, benchmarks page

### Milestone 4 — Polish

- [ ] Socket options (HWM, linger, identity)
- [ ] QUIC datagram variant for PUB/SUB
- [ ] qlog integration for debugging (RFC draft)
- [ ] Conformance tests
- [ ] Thesis: chapter 5 complete, chapter 6
- [ ] Docs: internals page, complete API reference

---

## 11. Key Commands

```bash
# Run all tests with race detector
go test ./... -race -v

# Build LaTeX thesis (requires texlive)
cd thesis && pdflatex main.tex && bibtex main && pdflatex main.tex && pdflatex main.tex

# Serve docs locally (requires Jekyll)
cd docs && bundle exec jekyll serve

# Check QUIC-go docs
go doc github.com/quic-go/quic-go
go doc github.com/quic-go/quic-go.Config
go doc github.com/quic-go/quic-go.Connection
```

---

## 12. Important Constraints & Rules

1. **Multiple transport layer support** — the library must primarly use QUIC as transport but it should be able to use TCP as an option, this will be used to test their efficiency.
2. **ZMTP 3.1 wire compatibility** — framing must be spec-compliant so a
   future interop layer with standard ZMQ is possible.
3. **Thread-safe** — all socket operations must be safe for concurrent use
   from multiple goroutines.
4. **Context cancellation** — all blocking operations (`Send`, `Recv`, `Accept`)
   must respect `context.Context`.
5. **Self-signed TLS by default** — generate a self-signed cert in `NewContext()`
   if none is provided; expose `TLSConfig` option for production use.
6. **No CGo** — pure Go only.
7. **Minimum Go version**: 1.21 (needed for `log/slog` and `quic-go` ≥ 0.42).
8. **Module path**: `github.com/<your-username>/zmqquic`

---

## 14. Thesis Update Checklist (run after every milestone)

- [ ] New implementation described in chapter 4 with code listing
- [ ] Design decisions justified with RFC citations
- [ ] Benchmark results added as table in chapter 5
- [ ] Benchmark plot added as figure in `thesis/figures/` (use gnuplot or matplotlib)
- [ ] Chapter 3 architecture diagram updated if topology changed
- [ ] `\today` date will update automatically on rebuild
- [ ] All `\cite{}` keys exist in `references.bib`
- [ ] LaTeX compiles without errors: `pdflatex main.tex`

---

_End of CLAUDE.md_
