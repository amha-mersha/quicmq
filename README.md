# QuicMQ

A lightweight, high-performance message broker library for Go, inspired by ZeroMQ but built on QUIC protocol. QuicMQ provides broker-less messaging patterns with built-in TLS encryption, efficient connection pooling, and support for multiple messaging patterns.

## Features

- **🔒 Encryption by Default**: TLS encryption built into QUIC, with option to disable for testing
- **⚡ Efficient Transport**: Reuses QUIC connections per destination, multiplexes messages over streams
- **🎯 Multiple Patterns**: Support for PUB/SUB (implemented), REQ/REP (planned)
- **🔧 Highly Configurable**: Customize timeouts, stream limits, buffer sizes, and more
- **📦 Zero Dependencies**: Minimal external dependencies beyond quic-go
- **🚀 Lightweight**: Designed for performance and low resource usage

## Architecture

QuicMQ leverages QUIC's layered architecture:

- **Transport Layer**: Single QUIC transport per destination address
- **Connections**: Persistent connections with automatic keepalive
- **Streams**: Individual messages sent over separate bidirectional streams
- **Multiplexing**: Multiple messaging patterns share the same connection

### Why QUIC?

- Built-in TLS 1.3 encryption (no separate TLS handshake)
- Reduced connection overhead compared to TCP
- Native multiplexing without head-of-line blocking
- Connection migration support
- Modern congestion control

## Installation

```bash
go get github.com/yourusername/quicmq
```

## Quick Start

### Publisher Example

```go
package main

import (
    "quicmq/lib"
    "time"
)

func main() {
    // Create context with default config
    ctx := quicmq.NewContext()
    
    // Create publisher socket
    socket, _ := ctx.NewSocket(quicmq.PUB)
    socket.Bind("quic://127.0.0.1:5000")
    
    // Publish messages
    for {
        socket.Send([]byte("Hello, World!"), 0)
        time.Sleep(1 * time.Second)
    }
}
```

### Subscriber Example

```go
package main

import (
    "fmt"
    "quicmq/lib"
)

func main() {
    // Create context
    ctx := quicmq.NewContext()
    
    // Create subscriber socket
    socket, _ := ctx.NewSocket(quicmq.SUB)
    socket.Connect("quic://127.0.0.1:5000")
    
    // Receive messages
    for {
        msg, _ := socket.Recv(0)
        fmt.Printf("Received: %s\n", msg)
    }
}
```

## Configuration

QuicMQ provides extensive configuration options:

```go
config := &quicmq.Config{
    MaxStreams:        100,                  // Max concurrent streams per connection
    MaxIdleTimeout:    30 * time.Second,     // Connection idle timeout
    HandshakeTimeout:  10 * time.Second,     // QUIC handshake timeout
    KeepAlivePeriod:   10 * time.Second,     // Keepalive interval
    DisableEncryption: false,                // Disable TLS (not recommended)
    MaxMessageSize:    10 * 1024 * 1024,     // 10MB max message size
    StreamBufferSize:  4096,                 // Stream buffer size
}

ctx := quicmq.NewContextWithConfig(config)
```

### Default Configuration

- **MaxStreams**: 100 concurrent streams
- **MaxIdleTimeout**: 30 seconds
- **HandshakeTimeout**: 10 seconds
- **KeepAlivePeriod**: 10 seconds
- **DisableEncryption**: false (encryption enabled)
- **MaxMessageSize**: 10MB
- **StreamBufferSize**: 4KB

## Messaging Patterns

### PUB/SUB (Publisher-Subscriber)

Publishers broadcast messages to all connected subscribers. This pattern is ideal for:
- Event broadcasting
- Real-time data distribution
- Fan-out architectures

```go
// Publisher
pub, _ := ctx.NewSocket(quicmq.PUB)
pub.Bind("quic://0.0.0.0:5000")
pub.Send([]byte("event data"), 0)

// Subscriber
sub, _ := ctx.NewSocket(quicmq.SUB)
sub.Connect("quic://server:5000")
msg, _ := sub.Recv(0)
```

### REQ/REP (Request-Reply) - Coming Soon

Request-reply pattern for synchronous RPC-style communication.

## Examples

See the `examples/` directory for complete working examples:

### CLI Tool (examples/pubsub_cli/)

```bash
# Terminal 1: Run subscriber
go run examples/pubsub_cli/main.go -mode sub -addr quic://127.0.0.1:5000

# Terminal 2: Run publisher
go run examples/pubsub_cli/main.go -mode pub -addr quic://127.0.0.1:5000
```

### Basic Example (examples/basic/)

```bash
# Terminal 1: Run the basic publisher
go run examples/basic/main.go

# Terminal 2: Subscribe to messages
go run examples/pubsub_cli/main.go -mode sub -addr quic://127.0.0.1:5555
```

#### CLI Options:
- `-mode` : pub or sub
- `-addr` : Address to bind/connect
- `-interval` : Message interval (publisher only)
- `-no-encrypt` : Disable encryption

## API Reference

### Context

```go
// Create new context with default config
ctx := quicmq.NewContext()

// Create context with custom config
ctx := quicmq.NewContextWithConfig(config)

// Create a socket
socket, err := ctx.NewSocket(quicmq.PUB)
```

### Socket

```go
// Bind to address (for servers/publishers)
err := socket.Bind("quic://0.0.0.0:5000")

// Connect to address (for clients/subscribers)
err := socket.Connect("quic://server:5000")

// Send message
err := socket.Send([]byte("message"), 0)

// Receive message
msg, err := socket.Recv(0)

// Close socket
err := socket.Close()
```

## Address Formats

QuicMQ supports flexible address formats:

- `quic://host:port` - QUIC protocol (recommended)
- `tcp://host:port` - ZeroMQ compatibility (maps to QUIC)
- `host:port` - Direct host:port

## Performance Considerations

1. **Connection Reuse**: QuicMQ reuses connections to the same destination
2. **Stream Pooling**: Each message uses a separate stream for parallelism
3. **Buffer Sizes**: Adjust `StreamBufferSize` based on your message patterns
4. **Max Streams**: Tune `MaxStreams` for your concurrent message load
5. **Timeouts**: Set appropriate timeouts for your network conditions

## Comparison with ZeroMQ

| Feature | QuicMQ | ZeroMQ |
|---------|--------|--------|
| Protocol | QUIC/UDP | TCP/IPC/... |
| Encryption | Built-in TLS 1.3 | Requires CurveZMQ |
| Connection Setup | 1-RTT | 3-RTT (TCP) |
| Multiplexing | Native QUIC streams | Manual |
| Head-of-line blocking | No | Yes (TCP) |
| Connection migration | Yes | No |

## Security

### Encryption

QuicMQ uses TLS 1.3 encryption by default through QUIC:
- Self-signed certificates generated automatically
- Perfect forward secrecy
- Modern cipher suites

For production, consider:
- Providing your own certificates
- Using proper CA-signed certificates
- Implementing certificate pinning

### Disabling Encryption

Only disable encryption for local testing:

```go
config := quicmq.DefaultConfig()
config.DisableEncryption = true
ctx := quicmq.NewContextWithConfig(config)
```

## Roadmap

- [x] PUB/SUB pattern
- [x] TLS encryption by default
- [x] Connection pooling
- [x] Configurable parameters
- [ ] REQ/REP pattern
- [ ] PUSH/PULL pattern
- [ ] Custom certificate support
- [ ] Connection statistics
- [ ] Compression support
- [ ] Message filtering/topics

## Acknowledgments

- Inspired by [ZeroMQ](https://zeromq.org/)
- Built on [quic-go](https://github.com/quic-go/quic-go)