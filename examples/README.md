# QuicMQ Examples

This directory contains example programs demonstrating how to use QuicMQ.

## Examples

### 1. PubSub CLI (`pubsub_cli/`)

A command-line tool that demonstrates the publisher-subscriber pattern.

#### Usage

**Start a subscriber:**
```bash
go run pubsub_cli/main.go -mode sub -addr quic://127.0.0.1:5000
```

**Start a publisher:**
```bash
go run pubsub_cli/main.go -mode pub -addr quic://127.0.0.1:5000 -interval 1s
```

#### Options

- `-mode` : Operation mode - `pub` (publisher) or `sub` (subscriber)
- `-addr` : Address to bind (publisher) or connect (subscriber)
- `-interval` : Time between messages (publisher only, default: 1s)
- `-no-encrypt` : Disable encryption (for testing only)

### 2. Basic Example (`basic/`)

A simple programmatic example showing how to use QuicMQ in code.

```bash
# Terminal 1: Run the basic publisher
go run basic/main.go

# Terminal 2: Run a subscriber to see the messages
go run pubsub_cli/main.go -mode sub -addr quic://127.0.0.1:5555
```

## Example Scenarios

#### Basic Pub/Sub

Terminal 1 (subscriber):
```bash
go run pubsub_cli/main.go -mode sub -addr quic://127.0.0.1:5000
```

Terminal 2 (publisher):
```bash
go run pubsub_cli/main.go -mode pub -addr quic://127.0.0.1:5000
```

#### Multiple Subscribers

You can run multiple subscribers that will all receive the same messages:

Terminal 1 (subscriber 1):
```bash
go run pubsub_cli/main.go -mode sub -addr quic://127.0.0.1:5000
```

Terminal 2 (subscriber 2):
```bash
go run pubsub_cli/main.go -mode sub -addr quic://127.0.0.1:5000
```

Terminal 3 (publisher):
```bash
go run pubsub_cli/main.go -mode pub -addr quic://127.0.0.1:5000
```

#### Fast Publishing

Publish messages every 100ms:
```bash
go run pubsub_cli/main.go -mode pub -addr quic://127.0.0.1:5000 -interval 100ms
```

#### Without Encryption (Testing Only)

Publisher:
```bash
go run pubsub_cli/main.go -mode pub -addr quic://127.0.0.1:5000 -no-encrypt
```

Subscriber:
```bash
go run pubsub_cli/main.go -mode sub -addr quic://127.0.0.1:5000 -no-encrypt
```

## Building Examples

Build the CLI tool:
```bash
go build -o pubsub pubsub_cli/main.go
```

Then run directly:
```bash
./pubsub -mode pub -addr quic://127.0.0.1:5000
```

## Notes

- The publisher generates random numbers and simulated temperature data
- Messages include timestamps for tracking delivery
- Both publisher and subscriber handle graceful shutdown with Ctrl+C
- The first few messages may be lost if subscribers connect after publisher starts
