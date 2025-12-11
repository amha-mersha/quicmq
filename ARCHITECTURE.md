# QuicMQ Implementation Summary

## What Was Implemented

### Core Architecture

**QuicMQ** is a lightweight, high-performance message broker library built on QUIC protocol. It provides ZeroMQ-style messaging patterns with modern encryption and efficient multiplexing.

### Key Components

#### 1. **Socket Interface** (`Socket`)
- Matches ZeroMQ API patterns for ease of adoption
- Methods: `Send()`, `Recv()`, `Bind()`, `Connect()`, `Subscribe()`, etc.
- Supports multiple socket types: PUB/SUB (implemented), REQ/REP (planned)

#### 2. **Context** (`quicMQContext`)
- Manages all sockets and transport connections
- Maintains transport pool indexed by address for connection reuse
- Thread-safe socket creation via mutex locks
- Handles graceful shutdown of all resources

#### 3. **Socket Configuration** (`socketConfig`)
- **maxBufferSize**: Max messages to buffer per stream (default: 100)
- **timeout**: Connection timeout (default: 30s)
- **sendTimeout/recvTimeout**: I/O operation timeouts (default: 5s)
- **bindAddr**: Address to bind to (publisher/replier sockets)
- **connectAddrs**: Addresses to connect to (subscriber/requester sockets)

#### 4. **Stream Management** (`StreamBuffer`)
```go
type StreamBuffer struct {
    stream *quic.Stream    // Pointer to active QUIC stream
    buffer [][]byte        // Message queue with maxBufferSize capacity
}
```
- Each stream has its own message buffer
- Buffers decouple message production from consumption
- Allows handling of multiple concurrent messages per stream

### Threading & Synchronization

**Mutex Usage:**
```go
type quicMQContext struct {
    sync.Mutex
    transports map[string]*quic.Transport
    sockets    map[SocketID]Socket
}
```

Why mutexes are needed:
- **Data Race Prevention**: Multiple goroutines may create sockets simultaneously
- **Transport Reuse**: Only one transport can be created per address
- **Safe State Management**: Prevents corruption of maps and counters
- **Graceful Shutdown**: Ensures all resources are closed atomically

Each socket also has its own `RWMutex` for the `activeStreams` map, allowing:
- Multiple goroutines to read active streams concurrently
- Exclusive write access when adding/removing streams

### Connection & Transport Model

**QUIC Transport Layer:**
- One UDP socket per bind address
- All connections on same address share one `quic.Transport`
- Reduces kernel overhead and improves performance
- Automatic multiplexing over a single UDP connection

**Connection Reuse:**
- Transport instances are cached in `quicMQContext`
- Multiple sockets can share the same transport
- Indexed by address string for O(1) lookup

**Stream Multiplexing:**
- Each message gets its own QUIC stream
- Streams are independent - no head-of-line blocking
- Streams are pooled in `activeStreams` map per socket

### TLS & Encryption

**By Design:**
- QUIC **requires** TLS 1.3 - it's built into the protocol
- No separate TLS handshake (1-RTT instead of 3-RTT)
- Encryption happens at transport layer

**Current Implementation:**
```go
func generateTLSConfig() *tls.Config {
    return &tls.Config{
        InsecureSkipVerify: true,  // Self-signed certs for testing
        NextProtos:         []string{"quicmq"},
    }
}
```

**Why No "Disable Encryption" Option:**
- QUIC fundamentally requires encryption
- Overhead is minimal (already in protocol)
- Self-signed certificates are auto-generated

### Functional Options Pattern

```go
type Option func(*socketConfig)

// Usage
socket, _ := ctx.NewSocket(PUB, 
    WithBind("quic://127.0.0.1:5000"),
    WithTimeout(10*time.Second),
)
```

**Benefits:**
- Flexible API without breaking changes
- Optional configuration (uses defaults)
- Type-safe compile-time checking
- Readable function composition

### Address Handling

Currently maps both `quic://` and `tcp://` prefixes to QUIC/UDP:
- `parseAddr()` normalizes addresses to `host:port` format
- All traffic uses QUIC over UDP (TCP prefix is for ZMQ compatibility)
- Validates addresses contain both host and port

### Message Flow

**Publishing (PUB):**
1. Socket binds to address
2. QUIC listener accepts incoming connections
3. New subscribers connect to socket
4. `Send()` broadcasts to all connected subscribers
5. Each subscriber gets its own QUIC stream
6. Messages are buffered in each stream's `StreamBuffer`

**Subscribing (SUB):**
1. Socket connects to publisher address
2. Maintains subscription list
3. `Recv()` reads from buffered streams
4. Can subscribe/unsubscribe to topics (planned)

## Performance Characteristics

- **Transport Reuse**: O(1) lookup, reduces system calls
- **Stream Pooling**: Each message = separate stream (no blocking)
- **Buffer Management**: Configurable buffer size prevents unbounded memory growth
- **Concurrent Access**: RWMutex allows simultaneous readers

## Testing & Examples

Two example programs demonstrate usage:

1. **pubsub_cli/** - Full-featured CLI tool with:
   - Publisher mode: generates random data continuously
   - Subscriber mode: receives and displays messages
   - Configurable timeouts, encryption, message intervals

2. **basic/** - Simple programmatic example showing:
   - Context and socket creation
   - Custom configuration
   - Basic publish loop

## Future Enhancements

- [ ] REQ/REP pattern (request-reply / RPC-style)
- [ ] PUSH/PULL pattern (pipeline)
- [ ] Topic-based filtering in PUB/SUB
- [ ] Custom certificate support for production
- [ ] Connection statistics & monitoring
- [ ] Message compression
- [ ] Automatic reconnection

## Design Decisions

1. **Multiple Endpoints Per Socket** (like ZMQ)
   - Allows one socket to bind/connect to multiple addresses
   - More flexible than single-endpoint design
   - Matches user expectations from ZMQ

2. **Stream Per Message**
   - Enables full parallelism without head-of-line blocking
   - Allows independent error handling per message
   - Leverages QUIC's multiplexing capabilities

3. **Self-Signed TLS by Default**
   - Encryption is necessary for QUIC
   - Self-signed certs are secure for internal networks
   - No manual CA management needed for testing

4. **Functional Options**
   - Cleaner API than builder pattern
   - Extensible without interface changes
   - Go idiomatic approach
