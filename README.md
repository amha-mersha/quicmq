# QuicMQ

A lightweight, high-performance message queue library for Go, inspired by [go-zeromq/zmq4](https://github.com/go-zeromq/zmq4) but built on QUIC protocol.

## Features

- **🎯 ZMQ4-style API** — Simple `NewPub(ctx)` / `NewSub(ctx)` constructors, no context/broker object needed
- **🔒 Encryption by Default** — TLS 1.3 encryption built into QUIC, no separate ZMTP layer
- **⚡ QUIC Transport** — Stream multiplexing, no head-of-line blocking
- **📡 Topic-based Pub/Sub** — Publisher-side topic filtering, just like ZeroMQ
- **🔌 Pluggable Transports** — QUIC by default, extensible via `RegisterTransport()`
- **📦 Minimal Dependencies** — Only `quic-go` as external dependency

## Quick Start

### Publisher

```go
pub := quicmq.NewPub(context.Background())
defer pub.Close()

pub.Listen("quic://0.0.0.0:9000")

for i := 0; ; i++ {
    pub.Send(quicmq.NewMsgString(fmt.Sprintf("weather temp=%d", 20+i%10)))
    time.Sleep(time.Second)
}
```

### Subscriber

```go
sub := quicmq.NewSub(context.Background())
defer sub.Close()

sub.Dial("quic://127.0.0.1:9000")
sub.SetOption(quicmq.OptionSubscribe, "weather")

for {
    msg, _ := sub.Recv()
    fmt.Printf("Received: %s\n", msg.Frames[0])
}
```

## Socket Types

| Type | Description |
|------|-------------|
| **PUB** | Publishes messages with topic-based filtering. Cannot receive. |
| **SUB** | Subscribes to topics via `SetOption`. Receives matching messages. |
| **XPUB** | Extended PUB — exposes subscription commands via `Recv()`. For proxy/broker devices. |
| **XSUB** | Extended SUB — sends subscriptions as raw messages via `Send()`. For proxy/broker devices. |

## TLS Configuration

```go
// Development (self-signed, auto-generated):
pub := quicmq.NewPub(ctx) // uses GenerateTLSConfig() by default

// Production:
tlsCfg, _ := quicmq.NewTLSConfig("server.crt", "server.key")
pub := quicmq.NewPub(ctx, quicmq.WithListenTLS(tlsCfg))

clientCfg, _ := quicmq.NewClientTLSConfig("ca.crt")
sub := quicmq.NewSub(ctx, quicmq.WithDialTLS(clientCfg))
```

## Pluggable Transports

```go
// Register a custom transport
quicmq.RegisterTransport("tcp", myTCPTransport{})

// Use with any socket
pub.Listen("tcp://0.0.0.0:9000")
```

## Running Examples

```bash
# Terminal 1: Publisher
go run example/pubsub/publisher/main.go

# Terminal 2: Subscriber
go run example/pubsub/subscriber/main.go
```

## Running Tests

```bash
go test -v -count=1
```

## Roadmap

- [x] PUB/SUB pattern with topic filtering
- [x] XPUB/XSUB pattern
- [x] Pluggable transport interface
- [x] TLS encryption by default
- [ ] REQ/REP pattern
- [ ] Connection pooling (QUICContext)