package main

import (
	"fmt"
	"log"
	"time"

	quicmq "quicmq/lib"
)

// This example shows basic programmatic usage of QuicMQ
// Run this and then run a subscriber to see messages
func main() {
	// Create a context with custom configuration
	config := quicmq.DefaultConfig()
	// You can customize QUIC settings like this:
	// config.QuicConfig.MaxIdleTimeout = 60 * time.Second
	// config.QuicConfig.KeepAlivePeriod = 15 * time.Second
	config.MaxMessageSize = 5 * 1024 * 1024 // 5MB max messages

	ctx := quicmq.NewContextWithConfig(config)

	// Create a publisher socket
	pubSocket, err := ctx.NewSocket(quicmq.PUB)
	if err != nil {
		log.Fatalf("Failed to create publisher: %v", err)
	}
	defer pubSocket.Close()

	// Bind to address
	if err := pubSocket.Bind("quic://127.0.0.1:5555"); err != nil {
		log.Fatalf("Failed to bind: %v", err)
	}

	fmt.Println("Publisher started on quic://127.0.0.1:5555")
	fmt.Println("Start a subscriber with: go run pubsub.go -mode sub -addr quic://127.0.0.1:5555")

	// Give subscribers time to connect
	time.Sleep(2 * time.Second)

	// Send some messages
	for i := 1; i <= 10; i++ {
		message := fmt.Sprintf("Message %d: Hello from basic example!", i)

		if err := pubSocket.Send([]byte(message), 0); err != nil {
			log.Printf("Failed to send: %v", err)
			continue
		}

		fmt.Printf("Sent: %s\n", message)
		time.Sleep(1 * time.Second)
	}

	fmt.Println("Done sending messages")
}
