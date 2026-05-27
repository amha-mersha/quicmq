// QuicMQ Presentation — Addis Ababa University Bachelor's Capstone
// Sophonias Demiss (UGR/5998/14) & Amha Mersha (UGR/9236/14)
// Supervisor: Mr. Kinde Mekuria

#import "@preview/touying:0.6.1": *
#import themes.metropolis: *

// ── Colours ──────────────────────────────────────────────────────────────────
#let qblue  = rgb("#0062cc")
#let tred   = rgb("#cc2200")
#let grn    = rgb("#1a7a1a")
#let amber  = rgb("#c07000")
#let bg     = rgb("#f5f7fa")

// ── Helpers ───────────────────────────────────────────────────────────────────
#let mbox(num, lbl, col: qblue, sub: none) = box(
  width: 100%, fill: col.lighten(91%), stroke: 0.8pt + col, radius: 5pt, inset: 10pt,
  align(center)[
    #text(size: 1.7em, weight: "bold", fill: col, num)
    #if sub != none [#linebreak()#text(size: 0.65em, fill: col.darken(15%), sub)]
    #linebreak()
    #text(size: 0.72em, fill: luma(55), lbl)
  ],
)

#let note(body, col: qblue) = block(
  fill: col.lighten(92%), stroke: (left: 3pt + col), inset: 8pt,
  radius: (right: 3pt), body,
)

#let two(l, r, ratio: (1fr, 1fr)) = grid(columns: ratio, gutter: 14pt, l, r)
#let three(a, b, c) = grid(columns: (1fr, 1fr, 1fr), gutter: 10pt, a, b, c)
#let four(a, b, c, d) = grid(columns: (1fr, 1fr, 1fr, 1fr), gutter: 8pt, a, b, c, d)

#let fig(path, caption: none, w: 100%) = {
  image(path, width: w)
  if caption != none {
    text(size: 0.65em, fill: luma(55), caption)
  }
}

// ── Presentation ──────────────────────────────────────────────────────────────
#show: metropolis-theme.with(
  aspect-ratio: "16-9",
  config-colors(
    primary: qblue,
    secondary: rgb("#4a90d9"),
    neutral-lightest: rgb("#f8fafc"),
    neutral-light: rgb("#e8edf4"),
    neutral-dark: rgb("#2c3e50"),
    neutral-darkest: rgb("#1a252f"),
  ),
  config-info(
    title: [*QuicMQ*: ZeroMQ Patterns over QUIC],
    subtitle: [Bachelor's Capstone — Computer Science],
    author: [Sophonias Demiss · Amha Mersha],
    date: [June 2026],
    institution: [Addis Ababa University],
  ),
)

// ════════════════════════════════════════════════════════════════════════════
#title-slide()

// ══════════════════════════════════════════════════════════════════
= Part I — Foundation & Design

// ── Slide: What We Built ─────────────────────────────────────────────────────
== What We Built

#two(
  [
    // Architecture diagram + stats line sits here, under the image
    #fig("../thesis/figures/architecture.png",
         caption: "7,800 lines · Go 1.22 · 40/40 tests · race-detector clean")
  ],
  [
    #text(size: 0.76em)[
    #note[
      *QuicMQ* is a pure-Go, brokerless messaging library implementing *ZMTP 3.1* over two transports — *QUIC* (primary) and *TCP* (comparison baseline).
      #v(4pt)
      No broker. No CGo. Thread-safe. Context-aware.
    ]]
    #v(8pt)
    #grid(
      columns: (1fr, 1fr),
      gutter: 8pt,
      box(fill: qblue.lighten(90%), stroke: 0.6pt + qblue, radius: 4pt, inset: 8pt)[
        #text(size: 0.74em)[
          *QUIC transport*\
          TLS 1.3 built-in\
          Stream multiplexing\
          Connection migration\
          0-RTT resumption\
          RFC 9221 datagrams
        ]
      ],
      box(fill: luma(242), stroke: 0.6pt + luma(180), radius: 4pt, inset: 8pt)[
        #text(size: 0.74em)[
          *TCP transport*\
          CURVE security\
          Curve25519 + NaCl\
          Baseline comparison\
          ZMTP compliant
        ]
      ],
    )
  ],
  ratio: (3fr, 2fr),
)

// ── Slide: Messaging Patterns ─────────────────────────────────────────────────
== 8 Messaging Patterns Implemented

#v(4pt)
#grid(
  columns: (1fr, 1fr, 1fr, 1fr),
  rows: (auto, auto),
  gutter: 8pt,

  box(fill: qblue.lighten(89%), stroke: 0.8pt + qblue, radius: 5pt, inset: 8pt)[
    #align(center)[
      #text(weight: "bold", fill: qblue, "PUB")
      #linebreak()
      #text(size: 0.72em)[Fan-out to all subs\ Topic prefix filter\ HWM drop policy\ Listen/Send only]
    ]
  ],
  box(fill: qblue.lighten(89%), stroke: 0.8pt + qblue, radius: 5pt, inset: 8pt)[
    #align(center)[
      #text(weight: "bold", fill: qblue, "SUB")
      #linebreak()
      #text(size: 0.72em)[Topic subscribe/cancel\ Auto-reconnect\ Exponential backoff\ Dial/Recv only]
    ]
  ],
  box(fill: grn.lighten(88%), stroke: 0.8pt + grn, radius: 5pt, inset: 8pt)[
    #align(center)[
      #text(weight: "bold", fill: grn, "REQ")
      #linebreak()
      #text(size: 0.72em)[Strict send→recv\ Auto delimiter frame\ Per-RPC QUIC stream\ Blocks until reply]
    ]
  ],
  box(fill: grn.lighten(88%), stroke: 0.8pt + grn, radius: 5pt, inset: 8pt)[
    #align(center)[
      #text(weight: "bold", fill: grn, "REP")
      #linebreak()
      #text(size: 0.72em)[Strict recv→send\ Strips delimiter\ Concurrent clients\ Listen/Recv/Send]
    ]
  ],

  box(fill: amber.lighten(88%), stroke: 0.8pt + amber, radius: 5pt, inset: 8pt)[
    #align(center)[
      #text(weight: "bold", fill: amber, "XPUB")
      #linebreak()
      #text(size: 0.72em)[Exposes raw sub events\ Proxy-friendly\ Multi-hop routing\ Listen + send]
    ]
  ],
  box(fill: amber.lighten(88%), stroke: 0.8pt + amber, radius: 5pt, inset: 8pt)[
    #align(center)[
      #text(weight: "bold", fill: amber, "XSUB")
      #linebreak()
      #text(size: 0.72em)[Explicit sub commands\ Proxy upstream\ Manual filtering\ Dial + recv]
    ]
  ],
  box(fill: rgb("#6b4fbb").lighten(80%), stroke: 0.8pt + rgb("#6b4fbb"), radius: 5pt, inset: 8pt)[
    #align(center)[
      #text(weight: "bold", fill: rgb("#6b4fbb"), "DatagramPub")
      #linebreak()
      #text(size: 0.72em)[RFC 9221 unreliable\ Payloads as datagrams\ Control via stream\ QUIC-only socket]
    ]
  ],
  box(fill: rgb("#6b4fbb").lighten(80%), stroke: 0.8pt + rgb("#6b4fbb"), radius: 5pt, inset: 8pt)[
    #align(center)[
      #text(weight: "bold", fill: rgb("#6b4fbb"), "DatagramSub")
      #linebreak()
      #text(size: 0.72em)[Subscribe on stream\ Receive as datagram\ No retransmit delay\ QUIC-only socket]
    ]
  ],
)
#v(4pt)
#align(center)[
  #text(size: 0.7em, fill: luma(55))[
    Common options: `WithAutomaticReconnect` · `WithReconnectInterval` · `WithDialTimeout` · `WithConnectionPool` · `WithDialTLS` · `WithTimeout`
  ]
]

// ── Slide: Throughput — data ──────────────────────────────────────────────────
== Throughput: QUIC vs TCP (Loopback Benchmarks)

#fig("../thesis/figures/throughput_comparison.png",
     caption: "PUB/SUB throughput: QUIC stream vs TCP vs QUIC datagram")
#v(6pt)
#two(
  [
    #v(2pt)
    #block(fill: bg, stroke: 0.7pt + luma(200), radius: 5pt, inset: 9pt)[
      #text(size: 0.72em, weight: "bold", "PUB/SUB loopback — 5-run median (Intel i3-1005G1, go test -bench)")
      #v(4pt)
      #table(
        columns: (auto, auto, auto, auto, auto),
        stroke: 0.5pt + luma(210), inset: 5pt,
        fill: (x, y) => if y == 0 { luma(235) } else if calc.odd(y) { white } else { luma(248) },
        table.header[*Transport*][*Size*][*ns/op*][*msg/s*][*MB/s*],
        text(fill: qblue, "QUIC stream"), "64 B",   "2 577",  text(fill: qblue, weight: "bold", "388 K"), "24.8",
        text(fill: tred,  "TCP CURVE"),  "64 B",   "4 524",  text(fill: tred,  "221 K"),                 "14.2",
        text(fill: qblue, "QUIC stream"), "1 KiB",  "10 067", "99 K",  text(fill: qblue, "102"),
        text(fill: tred,  "TCP CURVE"),  "1 KiB",  "5 892",  "170 K", text(fill: tred, "174"),
        text(fill: qblue, "QUIC stream"), "8 KiB",  "61 773", "16 K",  text(fill: qblue, "133"),
        text(fill: tred,  "TCP CURVE"),  "8 KiB",  "13 792", "73 K",  text(fill: tred, weight: "bold", "594"),
        text(fill: rgb("#6b4fbb"), "QUIC datagram"), "64 B", "6 173", "162 K", "10.4",
      )
    ]
  ],
  [
    #v(4pt)
    #note(col: grn)[
      #text(size: 0.72em)[
        *Loopback = best case for TCP, worst case for QUIC.* No packet loss, no mobility, single stream. Real-world concurrent / lossy conditions restore QUIC's multiplexing advantages — see physical LAN results.
      ]
    ]
    #v(8pt)
    #note(col: qblue)[
      #text(size: 0.73em)[
        *Why QUIC wins at 64 B:* Linux GSO batches multiple QUIC packets in one syscall. 18 allocs/op vs TCP's 27 allocs/op — less GC pressure.
      ]
    ]
    #note(col: tred)[
      #text(size: 0.73em)[
        *Why TCP wins at 8 KiB (loopback):* QUIC adds ~32 B of packet header + AEAD tag per packet, and fragments large messages across multiple packets (115 allocs/op vs TCP's 25). On a zero-loss loopback there is no retransmission — QUIC's independent-stream advantage is absent.
      ]
    ]
  ],
)

// ══════════════════════════════════════════════════════════════════
= Part II — QUIC Structural Advantages

// ── Slide: HoL Blocking — 1/2 ────────────────────────────────────────────────
== No Head-of-Line Blocking

#two(
  [
    #note(col: tred)[
      *TCP:* single ordered byte stream. One stalled packet blocks every message queued behind it — all concurrent subscribers on that connection stall together.
    ]
  ],
  [
    #note(col: qblue)[
      *QUIC:* each subscriber gets an independent stream. A retransmit in stream A never delays stream B. Subscribers receive at full speed regardless of others.
    ]
  ],
)
#v(14pt)
#text(size: 0.73em, weight: "bold")[Subscribers connected in 3-second startup window]
#text(size: 0.7em, fill: luma(50))[  — 20-publisher multinode scenario, real 1 Gbit/s LAN]
#v(8pt)
#align(center)[
#grid(columns: (1fr, 1fr), gutter: 12pt,
  box(fill: qblue.lighten(86%), stroke: 0.8pt + qblue, radius: 4pt, inset: 14pt)[
    #align(center)[
      #text(size: 0.75em, fill: luma(50), "QUIC")
      #linebreak()
      #text(size: 3em, weight: "bold", fill: qblue, "36")
      #linebreak()
      #text(size: 0.72em, "subscribers connected")
    ]
  ],
  box(fill: tred.lighten(86%), stroke: 0.8pt + tred, radius: 4pt, inset: 14pt)[
    #align(center)[
      #text(size: 0.75em, fill: luma(50), "TCP")
      #linebreak()
      #text(size: 3em, weight: "bold", fill: tred, "10")
      #linebreak()
      #text(size: 0.72em, "subscribers connected")
    ]
  ],
)
]
#v(10pt)
#align(center)[
  #box(fill: grn.lighten(88%), stroke: 0.6pt + grn, radius: 3pt, inset: (x: 14pt, y: 7pt))[
    #text(size: 0.9em, weight: "bold", fill: grn, "QUIC connects 3.6× more subscribers in the same window")
  ]
]

// ── Slide: HoL Blocking — 2/2 ────────────────────────────────────────────────
== No Head-of-Line Blocking

#two(
  [
    #note(col: amber)[
      #text(size: 0.76em)[
        *Why the gap?* TCP CURVE requires 3 round-trips per handshake (+2 RTT vs QUIC's 1-RTT TLS 1.3). Under rapid concurrent connections, the server accept-queue saturates before most TCP subscribers finish authenticating. QUIC's lighter handshake absorbs the burst and connects 3.6× more subscribers in the same 3-second window.
      ]
    ]
    #v(10pt)
    #note(col: qblue)[
      #text(size: 0.76em)[
        *At scale:* the accept-queue bottleneck compounds. Each additional subscriber on TCP must wait for preceding handshakes to drain the queue; QUIC subscribers proceed in parallel on independent streams within the same UDP flow.
      ]
    ]
  ],
  [
    #fig("../thesis/figures/fanout_scaling.png",
         caption: "Fan-out scaling: concurrent subscribers vs time (real LAN, 20 publishers)")
  ],
)

// ── Slide: Connection Migration ──────────────────────────────────────────────
== Connection Migration (RFC 9000 §9)

    #align(center)[
    #fig("../thesis/figures/connection_migration.png",
         caption: "RFC 9000 §9 path migration: CID persists across IP/port change", w:80%)
    ]
    #two(
      [
    #note(col: tred)[
      #text(size: 0.76em)[
      *TCP* identifies a connection by the 4-tuple (src IP, src port, dst IP, dst port). Any address change — WiFi to LTE, NAT rebind, load-balancer failover — *immediately resets the connection*. The application must detect this, reconnect, re-authenticate, and re-subscribe from scratch.
    ]
    ]
      ],
      [
    #note(col: qblue)[
      #text(size: 0.76em)[
      *QUIC* identifies connections by *opaque Connection IDs*, not by IP:port. When a client's address changes, QUIC sends a PATH_CHALLENGE frame on the new path; the server validates with PATH_RESPONSE and migrates traffic seamlessly — *zero application-level reconnect*.
    ]
    ]
      ]
    )
    #align(center)[
    #block(fill: bg, stroke: 0.7pt + luma(200), radius: 5pt, inset: 9pt)[
      #text(size: 0.72em, weight: "bold", "Measured outcome")
      #v(4pt)
      #grid(columns: (1fr, 1fr), gutter: 8pt,
        box(fill: qblue.lighten(86%), stroke: 0.8pt + qblue, radius: 4pt, inset: 8pt)[
          #align(center)[
            #text(size: 0.75em, fill: luma(50), "QUIC on address change")
            #linebreak()
            #text(size: 1.1em, weight: "bold", fill: qblue, "0 dropped messages")
            #linebreak()
            #text(size: 0.7em, "Subscription survives\nNo reconnect needed")
          ]
        ],
        box(fill: tred.lighten(86%), stroke: 0.8pt + tred, radius: 4pt, inset: 8pt)[
          #align(center)[
            #text(size: 0.75em, fill: luma(50), "TCP on address change")
            #linebreak()
            #text(size: 1.1em, weight: "bold", fill: tred, "Connection reset")
            #linebreak()
            #text(size: 0.7em, "All in-flight messages lost\nFull re-handshake required")
          ]
        ],
      )
      #v(4pt)
      #text(size: 0.7em, fill: luma(50), "Verified by TestConnectionMigration and TestConnectionMigrationWithSocket scenario tests.")
    ]
    ]

// ── Slide: 0-RTT Session Resumption ─────────────────────────────────────────
== 0-RTT Session Resumption

#align(center)[
  #fig("../thesis/figures/0rtt_timeline.png",
        caption: "Cold 1-RTT handshake (left) vs 0-RTT resumption (right) — QUIC connection timelines", w:90%)
]
#two(
  [
    #note(col: tred)[
      *TCP + CURVE:* every reconnect requires a full Diffie-Hellman exchange (+2 RTT) before any application data can flow. There is no session ticket concept in ZMTP/CURVE.
    ]
    #v(8pt)
    #note(col: qblue)[
      *QUIC (TLS 1.3):* after the first connection, the server issues a session ticket. On reconnect the client embeds ZMTP application data in the very first packet — *zero additional round-trips* before messaging resumes.
    ]
  ],
  [
      #grid(columns: (1fr, 1fr), gutter: 8pt,
        box(fill: tred.lighten(86%), stroke: 0.8pt + tred, radius: 4pt, inset: 8pt)[
          #align(center)[
            #text(size: 0.75em, fill: luma(50), "TCP CURVE reconnect")
            #linebreak()
            #text(size: 1.1em, weight: "bold", fill: tred, "Full handshake")
            #linebreak()
            #text(size: 0.7em, "Every reconnect\n+2 RTT always")
          ]
        ],
        box(fill: qblue.lighten(86%), stroke: 0.8pt + qblue, radius: 4pt, inset: 8pt)[
          #align(center)[
            #text(size: 0.75em, fill: luma(50), "QUIC 0-RTT reconnect")
            #linebreak()
            #text(size: 1.1em, weight: "bold", fill: qblue, "sub-1 ms")
            #linebreak()
            #text(size: 0.7em, "Session ticket reused\n0 extra RTT")
          ]
        ],
      )
  ]
)

// ── Slide: Connection Pool ───────────────────────────────────────────────────
== Connection Pool: Stream Reuse
#two(
  [
    #fig("../thesis/figures/connection_pool.png",
         caption: "Pool benchmark: 20 dials unpooled vs pooled — dial #1 is handshake, #2-20 are stream opens")
  ],
  [
    #block(fill: bg, stroke: 0.7pt + luma(200), radius: 5pt, inset: 9pt)[
      #text(size: 0.72em, weight: "bold", "Loopback benchmark — 5-run median")
      #v(6pt)
      #grid(columns: (1fr, 1fr), gutter: 8pt,
        box(fill: qblue.lighten(86%), stroke: 0.8pt + qblue, radius: 4pt, inset: 10pt)[
          #align(center)[
            #text(size: 0.75em, fill: luma(50), "Pooled dial (stream open)")
            #linebreak()
            #text(size: 2em, weight: "bold", fill: qblue, "0.217 ms")
            #linebreak()
            #text(size: 0.7em, "217 354 ns/op\nNo handshake")
          ]
        ],
        box(fill: tred.lighten(86%), stroke: 0.8pt + tred, radius: 4pt, inset: 10pt)[
          #align(center)[
            #text(size: 0.75em, fill: luma(50), "Unpooled dial (full handshake)")
            #linebreak()
            #text(size: 2em, weight: "bold", fill: tred, "3.621 ms")
            #linebreak()
            #text(size: 0.7em, "3 620 907 ns/op\nFull TLS 1.3")
          ]
        ],
      )
      #v(6pt)
      #align(center)[
        #box(fill: grn.lighten(88%), stroke: 0.8pt + grn, radius: 4pt, inset: (x: 12pt, y: 6pt))[
          #text(size: 1em, weight: "bold", fill: grn, "16.7× faster per dial")
        ]
      ]
    ]
  ],
  ratio: (3fr, 2fr),
)
#two(
  [
    #note(col: tred)[
      *TCP* cannot multiplex streams on a single connection. Each logical channel needs its own socket, its own CURVE handshake. Microservices with frequent connect/disconnect cycles pay the full cost every time.
    ]
  ],
  [
    #note(col: qblue)[
      *QUIC* multiplexes thousands of independent streams over one UDP flow. The `ConnectionPool` caches live QUIC connections keyed by `host:port`. New dials open a *stream* on the existing connection — no TLS handshake, no UDP packets for setup.
    ]
  ]
)

// ── Slide: Datagram Support - 1/2 ──────────────────────────────────────────────────
== DatagramPub/Sub vs QUIC Stream
#two(
  [
    #fig("../thesis/figures/datagram_topology.png",
         caption: "DatagramPub/Sub: control on reliable stream, data as RFC 9221 datagrams — one QUIC connection")
  ],
  [
    #note(col: rgb("#6b4fbb"))[
      #text(size: 0.72em)[
        *Key differentiator vs raw UDP:* DatagramPub rides an *existing encrypted QUIC connection* — no separate socket, no separate TLS, no extra port. Subscription control travels on a reliable stream; data payloads travel as RFC 9221 datagrams. One UDP socket carries both.
      ]
    ]
    #v(8pt)
    #text(size: 0.71em)[
      *Use datagram when:* IoT telemetry, game state, live video metadata — freshness matters more than completeness, payload fits in ~1 200 B.\
      *Use stream when:* guaranteed delivery required, or payload exceeds MTU.
    ]
  ],
  ratio: (3fr, 2fr),
)

// ── Slide: Datagram Support - 2/2 ──────────────────────────────────────────────────
== DatagramPub/Sub vs QUIC Stream
#align(center)[
    #block(fill: bg, stroke: 0.7pt + luma(200), radius: 5pt, inset: 6pt)[
      #text(size: 0.82em, weight: "bold", "Loopback benchmark comparison — same QUIC connection, different delivery path")
      #v(4pt)
      #set text(size: 0.74em)
      #table(
        columns: (auto, auto, auto, auto),
        stroke: 0.5pt + luma(210), inset: 4pt,
        fill: (x, y) => if y == 0 { luma(235) } else if calc.odd(y) { white } else { luma(248) },
        table.header[*Property*][*QUIC Stream*][*QUIC Datagram*][*Winner*],
        [msg/s at 64 B],   text(fill: qblue, "388 K"),  "162 K",  text(fill: qblue, "Stream"),
        [msg/s at 1 KiB],  "99 K",   text(fill: grn, "101 K"),  "≈ tie",
        [allocs/op 64 B],  "18",     text(fill: grn, "22"),     "Stream",
        [allocs/op 1 KiB], "40",     text(fill: grn, "22"),     text(fill: grn, "Datagram"),
        [Under 5% loss],   "0 gaps",  "~5% gaps",               text(fill: qblue, "Stream"),
        [p99 under loss],  "Higher",  text(fill: grn, "Lower"),  text(fill: grn, "Datagram"),
        [Max payload],     "Unlimited", "~1 200 B",              "Stream",
        [Delivery],        "Guaranteed", "Best-effort",          "Depends",
      )
    ]
    #block(fill: bg, stroke: 0.7pt + luma(200), radius: 5pt, inset: 9pt)[
      #text(size: 0.72em, weight: "bold", "Real LAN measurement (1 dpub, 5 dsubs, 256 B, 30 s)")
      #v(4pt)
      #grid(columns: (1fr, 1fr), gutter: 6pt,
        box(fill: rgb("#6b4fbb").lighten(84%), stroke: 0.6pt + rgb("#6b4fbb"), radius: 3pt, inset: 7pt)[
          #align(center)[#text(size: 0.7em, fill: luma(50), "Baseline rate")
          #linebreak()#text(size: 1.2em, weight: "bold", fill: rgb("#6b4fbb"), "906 msg/s")]
        ],
        box(fill: grn.lighten(84%), stroke: 0.6pt + grn, radius: 3pt, inset: 7pt)[
          #align(center)[#text(size: 0.7em, fill: luma(50), "Sequence gaps")
          #linebreak()#text(size: 1.2em, weight: "bold", fill: grn, "0")]
        ],
      )
    ]
    ]

// ══════════════════════════════════════════════════════════════════
= Part III — Results & Validation

// ── Slide: Physical LAN — latency ────────────────────────────────────────────
== Physical LAN: REQ/REP Tail Latency

#align(center)[
  #fig("../thesis/figures/phys_comparison.png",
      caption: "Physical LAN: (a) p95 latency  (b) p99 latency  (c) PUB/SUB messages delivered")
],
#two(
  [
    #v(8pt)
    #block(fill: qblue.lighten(90%), stroke: (left: 3pt + qblue), inset: 9pt, radius: (right: 4pt))[
      #text(size: 0.73em)[
        *Baseline p50 is identical* (~51 ms) — both transports have the same RTT on a 1 Gbit/s LAN.

        *The difference is in the tail:* at baseline (5 concurrent workers), QUIC p99 is *67.6 ms* vs TCP *135.9 ms* — a *2× improvement*. This is head-of-line blocking: one stalled TCP response delays all 5 workers on that connection. QUIC isolates each REQ/REP on its own stream.

        At stress (20 workers) the gap narrows because the bottleneck shifts to per-request CPU, not in-connection queuing.
      ]
    ]
    #v(6pt)
    #align(center)[
      #box(fill: grn.lighten(88%), stroke: 0.8pt + grn, radius: 4pt, inset: (x: 12pt, y: 6pt))[
        #text(size: 0.9em, weight: "bold", fill: grn, "QUIC p99: 2× lower tail latency at baseline concurrency")
      ]
    ]
  ],[
    #block(fill: bg, stroke: 0.7pt + luma(200), radius: 5pt, inset: 9pt)[
      #text(size: 0.72em, weight: "bold", "REQ/REP latency — real 1 Gbit/s LAN, 256 B payload")
      #text(size: 0.69em, fill: luma(50), " (Laptop A server ↔ Laptop B client, 30 s run)")
      #v(5pt)
      #table(
        columns: (auto, auto, auto, auto, auto, auto),
        stroke: 0.5pt + luma(210), inset: 4pt,
        fill: (x, y) => if y == 0 { luma(235) } else if calc.odd(y) { white } else { luma(248) },
        table.header[*Scenario*][*Transport*][*req/s*][*p50*][*p95*][*p99*],
        "Baseline (5)", text(fill: qblue, "QUIC"), "95.6",  "51.2", "54.2", text(fill: qblue, weight: "bold", "67.6 ms"),
        "Baseline (5)", text(fill: tred,  "TCP"),  "91.6",  "51.3", "64.1", text(fill: tred,  weight: "bold", "135.9 ms"),
        "Stress (20)",  text(fill: qblue, "QUIC"), "365.0", "51.2", "54.4", "57.4 ms",
        "Stress (20)",  text(fill: tred,  "TCP"),  "382.2", "51.2", "55.0", "62.7 ms",
      )
    ]
  ]
)

// ── Slide: Physical LAN — PUB/SUB ────────────────────────────────────────────
== Physical LAN: PUB/SUB Fan-out
#two(
  [
    #fig("../thesis/figures/fanout_scaling.png",
         caption: "Subscriber fan-out over time — QUIC reaches 36, TCP stalls at 10")
  ],
  [
    #note(col: qblue)[
      #text(size: 0.72em)[
        QUIC connects 3.6× more subscribers because CURVE's 3 extra RTTs cause TCP's accept-queue to saturate under rapid concurrent connections. QUIC's TLS 1.3 (1-RTT) handles the burst gracefully.
      ]
    ]
    #v(6pt)
    #block(fill: bg, stroke: 0.7pt + luma(200), radius: 5pt, inset: 9pt)[
      #text(size: 0.72em, weight: "bold", "PUB/SUB — subscribers connected in 3-second startup window")
      #v(5pt)
      #set text(size: 0.70em)
      #table(
        columns: (auto, auto, auto, auto, auto),
        stroke: 0.5pt + luma(210), inset: 4pt,
        fill: (x, y) => if y == 0 { luma(235) } else if calc.odd(y) { white } else { luma(248) },
        table.header[*Scenario*][*Transp.*][*Pubs*][*Connected subs*][*Received*],
        "Baseline",  text(fill: qblue, "QUIC"), "4", text(fill: qblue, "5"),  "39 643",
        "Baseline",  text(fill: tred,  "TCP"),  "4", text(fill: tred,  "2"),  "15 961",
        "Highrate",  text(fill: qblue, "QUIC"), "4", text(fill: qblue, "7"),  "215 323",
        "Highrate",  text(fill: tred,  "TCP"),  "4", text(fill: tred,  "2"),  "60 930",
        "Multinode", text(fill: qblue, "QUIC"), "20", text(fill: qblue, weight: "bold", "36"), "106 762",
        "Multinode", text(fill: tred,  "TCP"),  "20", text(fill: tred,  weight: "bold", "10"), "21 471",
      )
    ]
  ],
  ratio: (5fr, 4fr),
)

#two(
  [
    #fig("../thesis/figures/mesh_results.png",
         caption: "4-node Mininet mesh: latency under loss + jitter impairments", w: 100%)
  ],
  [
    #stack(
  dir: ttb,
  spacing: 0.5em,
  mbox("2×",   "p99 latency\n(67.6 vs 135.9 ms)", col: qblue),
  mbox("3.6×", "more subs\nconnected", col: grn),
  mbox("16.7×","pool dial\nspeedup", col: amber),
  mbox("906/s","datagram\nreal LAN", col: rgb("#6b4fbb")),
)
  ],
  ratio: (5fr, 1fr),
)

// ── Slide: Tests ──────────────────────────────────────────────────────────────
== Test Suite & Simulation Methods
#align(center)[
  #fig("../thesis/figures/scenario_pubsub.png",
        caption: "Docker scenario: PUB/SUB under 5 network impairment conditions")
],
#two(
  [
    #block(fill: bg, stroke: 0.7pt + luma(200), radius: 5pt, inset: 9pt)[
      #text(size: 0.74em, weight: "bold", "go test ./... -race  →  40/40 PASS")
      #v(4pt)
      #set text(size: 0.70em)
      #table(
        columns: (1fr, auto, auto),
        stroke: 0.5pt + luma(210), inset: 5pt,
        fill: (x, y) => if y == 0 { luma(235) } else if calc.odd(y) { white } else { luma(248) },
        table.header[*Category*][*Tests*][*Status*],
        [Unit — frame codec, greeting, CURVE, topic filter], "12", text(fill: grn, "PASS"),
        [Integration — all 8 socket types end-to-end],      "20", text(fill: grn, "PASS"),
        [Scenario — migration, 0-RTT, stateless reset, QLOG], "8", text(fill: grn, "PASS"),
        [Race detector], "all", text(fill: grn, "clean"),
        text(weight: "bold", "Total"), text(weight: "bold", "40"), text(weight: "bold", fill: grn, "All PASS"),
      )
    ]
  ],
  [
    #block(fill: bg, stroke: 0.7pt + luma(200), radius: 5pt, inset: 9pt)[
      #text(size: 0.73em, weight: "bold", "Three simulation environments")
      #block(fill: qblue.lighten(91%), stroke: (left: 2.5pt + qblue), inset: 6pt, radius: (right: 3pt))[
        #text(size: 0.71em)[
          *Docker Compose + tc-netem*: 5 conditions — baseline, 1% loss, 50 ms jitter, 10 Mbps cap, loss+jitter. 30 s per run, JSON result files.
        ]
      ]
      #block(fill: grn.lighten(91%), stroke: (left: 2.5pt + grn), inset: 6pt, radius: (right: 3pt))[
        #text(size: 0.71em)[
          *Mininet 4-node fat-tree*: h1/h3 = pubs, h2/h4 = subs. Configurable netem on WAN uplink. Per-host process isolation.
        ]
      ]
      #block(fill: amber.lighten(90%), stroke: (left: 2.5pt + amber), inset: 6pt, radius: (right: 3pt))[
        #text(size: 0.71em)[
          *Physical LAN (primary)*: Laptop A ↔ Laptop B, 1 Gbit/s, 256 B payload, 30 s. Both idle, same binary.
        ]
      ]
    ]
  ],
  ratio: (3fr, 2fr),
)


// ── Slide: Proposal vs Delivered ─────────────────────────────────────────────
== Proposal vs Delivered

#v(4pt)
#block(fill: bg, stroke: 0.7pt + luma(200), radius: 5pt, inset: 8pt)[
  #text(size: 0.73em, weight: "bold", "All project objectives from the thesis proposal — outcome:")
  #v(4pt)
  #set text(size: 0.80em)
  #table(
    columns: (1.8fr, auto, 1fr),
    stroke: 0.5pt + luma(210), inset: 5pt,
    fill: (x, y) => if y == 0 { luma(235) } else if calc.odd(y) { white } else { luma(248) },
    table.header[*Objective*][*Status*][*Evidence*],
    [ZMTP 3.1 framing over QUIC and TCP],           text(fill: grn, "✓ Done"), [FR-8, 40 tests],
    [PUB/SUB · REQ/REP · XPUB/XSUB socket types],  text(fill: grn, "✓ Done"), [FR-1–4, integration tests],
    [DatagramPub/DatagramSub (RFC 9221)],            text(fill: grn, "✓ Done"), [FR-5–6, phys LAN],
    [CURVE security on TCP (Curve25519 + NaCl)],    text(fill: grn, "✓ Done"), [FR-9, unit tests],
    [QUIC connection migration (RFC 9000 §9)],       text(fill: grn, "✓ Done"), [FR-10, TestConnectionMigration],
    [0-RTT session resumption + fallback],           text(fill: grn, "✓ Done"), [FR-11, Test0RTTSessionResumption],
    [Connection pool — target ≥10× speedup],         text(fill: grn, "✓ 16.7×"), [FR-13, bench],
    [GSO + DPLPMTUD for max QUIC throughput],        text(fill: grn, "✓ Done"), [auto-enabled, unit test],
    [QLOG structured tracing per connection],         text(fill: grn, "✓ Done"), [FR-12, TestQlogViaEnvVar/Option],
    [Micro-benchmarks + multi-node + physical LAN],  text(fill: grn, "✓ Done"), [Docker · Mininet · Pi 5],
  )
]
#v(4pt)
#align(center)[
  #box(fill: grn.lighten(88%), stroke: 0.8pt + grn, radius: 5pt, inset: (x: 14pt, y: 7pt))[
    #text(weight: "bold", fill: grn, "All 13 functional requirements (FR-1–FR-13) implemented and tested. 0 outstanding items.")
  ]
]

// ── Slide: QUIC vs TCP Summary ────────────────────────────────────────────────
== When QUIC Wins · When TCP Wins

#v(4pt)
#block(fill: bg, stroke: 0.7pt + luma(200), radius: 5pt, inset: 9pt)[
  #set text(size: 0.80em)
  #table(
    columns: (1.4fr, auto, 1fr),
    stroke: 0.5pt + luma(210), inset: 5pt,
    fill: (x, y) => if y == 0 { luma(235) } else if calc.odd(y) { white } else { luma(248) },
    table.header[*Condition*][*Winner*][*Measured result*],
    [Small messages (64 B), loopback],       text(fill: qblue, "QUIC"), [1.76× msg/s  (388 K vs 221 K)],
    [Bulk transfer (8 KiB), loopback],        text(fill: tred,  "TCP"),  [4.5× MB/s  (594 vs 133 MB/s)],
    [REQ/REP p99, real LAN, 5 workers],       text(fill: qblue, weight: "bold", "QUIC"), text(weight: "bold", [2× lower  (67.6 ms vs 135.9 ms)]),
    [Concurrent subscriber registration],     text(fill: qblue, weight: "bold", "QUIC"), text(weight: "bold", [3.6× more  (36 vs 10)]),
    [Frequent connect/disconnect],            text(fill: qblue, "QUIC"), [16.7× faster dial (0.217 vs 3.621 ms)],
    [Unreliable + reliable on one port],      text(fill: qblue, "QUIC"), [No TCP equivalent — RFC 9221 datagrams],
    [Network address / mobility change],      text(fill: qblue, "QUIC"), [Migration survives; TCP connection resets],
    [Reconnect after server restart],         text(fill: qblue, "QUIC"), [0-RTT sub-1 ms; TCP re-handshakes always],
    [Single-stream bulk file transfer],       text(fill: tred,  "TCP"),  [Lower per-packet overhead at large sizes],
    [Strict firewalls / UDP blocked],         text(fill: tred,  "TCP"),  [TCP traverses all middleboxes],
  )
]
#v(5pt)
#note(col: amber)[
  #text(size: 0.71em)[*Loopback bias (rows 1–2):* zero-loss, no mobility — conditions where QUIC was designed to shine are absent. Physical LAN rows 3–4 are the thesis's primary validation.]
]

// ── Final Slide ───────────────────────────────────────────────────────────────
== Summary

#two(
  [
    #block(fill: qblue.lighten(92%), stroke: 0.8pt + qblue, radius: 6pt, inset: 12pt)[
      #align(center)[
        #text(size: 1.1em, weight: "bold", fill: qblue, "QuicMQ")
        #text(size: 0.78em, fill: luma(40), " — ZeroMQ patterns over QUIC")
        #v(8pt)
        #four(
          mbox("40/40", "tests pass", col: grn),
          mbox("8", "socket\ntypes", col: qblue),
          mbox("2×",   "p99 latency\n(real LAN)", col: qblue),
          mbox("16.7×","pool\nspeedup", col: amber),
        )
        #v(8pt)
        #text(size: 0.74em)[
          *Sophonias Demiss* — UGR/5998/14 #linebreak()
          *Amha Mersha* — UGR/9236/14 #linebreak()
          Supervisor: Mr. Kinde Mekuria #linebreak()
          Addis Ababa University · June 2026
        ]
      ]
    ]
  ],
  [
    #block(fill: bg, stroke: 0.7pt + luma(200), radius: 6pt, inset: 10pt)[
      #text(size: 0.78em, weight: "bold", "Key takeaways")
      #v(6pt)
      #text(size: 0.75em)[
        *1.* QUIC's per-stream independence eliminates head-of-line blocking — the primary cause of TCP tail latency under concurrent load (2× p99 on real LAN).

        #v(4pt)
        *2.* Three capabilities have no TCP equivalent: connection migration, 0-RTT resumption, and unreliable datagrams on one encrypted connection.

        #v(4pt)
        *3.* TCP wins on single-stream bulk throughput (loopback). The right choice depends on the workload: mobility and concurrency → QUIC; raw bulk → TCP.

        #v(4pt)
        *4.* ZMTP 3.1 framing over both transports keeps future interop with standard ZeroMQ nodes open without any changes.
      ]
    ]
    #v(6pt)
    #align(center)[
      #box(fill: grn.lighten(88%), stroke: 0.8pt + grn, radius: 5pt, inset: (x: 14pt, y: 8pt))[
        #text(weight: "bold", fill: grn, "Demo follows →")
      ]
    ]
  ],
)
