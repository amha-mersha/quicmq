# QuicMQ

A lightweight, high-performance message queue library for Go, inspired by [go-zeromq/zmq4](https://github.com/go-zeromq/zmq4) but built on QUIC protocol.

## Features

- **🎯 ZMQ4-style API** — Simple `NewPub(ctx)` / `NewSub(ctx)` constructors, no context/broker object needed
- **🔒 Encryption by Default** — TLS 1.3 encryption built into QUIC
- **⚡ QUIC Transport** — Stream multiplexing, no head-of-line blocking
- **📡 Topic-based Pub/Sub** — Publisher-side topic filtering, just like ZeroMQ
- **🔌 Pluggable Transports** — QUIC by default, extensible via `RegisterTransport()`
- **📦 Minimal Dependencies** — Only `quic-go` as external dependency

## Quick Start

### Publisher

```go
package main

import (
    "context"
    "fmt"
    "quicmq"
    "time"
)

func main() {
    pub := quicmq.NewPub(context.Background())
    defer pub.Close()

    pub.Listen("quic://0.0.0.0:9000")

    for i := 0; ; i++ {
        pub.Send(quicmq.NewMsgString(fmt.Sprintf("weather temp=%d", 20+i%10)))
        time.Sleep(time.Second)
    }
}
```

### Subscriber

```go
package main

import (
    "context"
    "fmt"
    "quicmq"
)

func main() {
    sub := quicmq.NewSub(context.Background())
    defer sub.Close()

    sub.Dial("quic://127.0.0.1:9000")
    sub.SetOption(quicmq.OptionSubscribe, "weather")

    for {
        msg, _ := sub.Recv()
        fmt.Printf("Received: %s\n", msg.Frames[0])
    }
}
```

## Architecture

QuicMQ follows zmq4's layered architecture:

| Layer | Components |
|-------|------------|
| **Socket** | `NewPub()`, `NewSub()` — user-facing socket types |
| **Base Socket** | `socket.go` — Listen/Dial/Send/Recv, connection management |
| **I/O Pools** | `msgio.go` — reader/writer pools for fan-out |
| **Connection** | `conn.go` — length-prefixed framing, subscription tracking |
| **Transport** | `transport.go` — pluggable interface, `transport_quic.go` — QUIC impl |

### Pluggable Transports

QuicMQ ships with QUIC transport by default. Additional transports can be added:

```go
// Register a custom transport
quicmq.RegisterTransport("tcp", myTCPTransport{})

// List all registered transports
fmt.Println(quicmq.Transports()) // ["quic", "tcp"]
```

### Topic Filtering

Subscribers filter messages by topic prefix (same as ZeroMQ):

```go
sub.SetOption(quicmq.OptionSubscribe, "weather")  // only "weather..." messages
sub.SetOption(quicmq.OptionSubscribe, "")          // all messages
sub.SetOption(quicmq.OptionUnsubscribe, "weather") // stop receiving "weather..."
```

## Configuration

```go
pub := quicmq.NewPub(ctx,
    quicmq.WithTimeout(10 * time.Second),
    quicmq.WithDialerRetry(500 * time.Millisecond),
    quicmq.WithDialerMaxRetries(5),
    quicmq.WithServerTLS(myTLSConfig),
)
```

## Running Examples

```bash
# Terminal 1: Publisher
go run examples/pubsub/publisher/main.go

# Terminal 2: Subscriber
go run examples/pubsub/subscriber/main.go
```

## Running Tests

```bash
go test -v -run TestPubSub -count=1
```

## Roadmap

- [x] PUB/SUB pattern with topic filtering
- [x] Pluggable transport interface
- [x] TLS encryption by default
- [ ] REQ/REP pattern
- [ ] XPUB/XSUB pattern
- [ ] Connection pooling (QUICContext)
- [ ] Custom certificate support

## Acknowledgments

- API inspired by [go-zeromq/zmq4](https://github.com/go-zeromq/zmq4)
- Built on [quic-go](https://github.com/quic-go/quic-go)