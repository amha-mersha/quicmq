package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	quicmq "quicmq/lib"
)

func main() {
	// Parse command line arguments
	mode := flag.String("mode", "pub", "Mode: pub (publisher) or sub (subscriber)")
	address := flag.String("addr", "quic://127.0.0.1:5000", "Address to bind/connect (e.g., quic://127.0.0.1:5000)")
	interval := flag.Duration("interval", 1*time.Second, "Interval between messages (publisher only)")
	skipCertVerify := flag.Bool("skip-verify", true, "Skip TLS certificate verification (allows self-signed certs)")
	flag.Parse()

	// Create context with config
	config := quicmq.DefaultConfig()
	config.SkipCertVerification = *skipCertVerify

	ctx := quicmq.NewContextWithConfig(config)

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	switch *mode {
	case "pub":
		runPublisher(ctx, *address, *interval, sigChan)
	case "sub":
		runSubscriber(ctx, *address, sigChan)
	default:
		fmt.Fprintf(os.Stderr, "Invalid mode: %s. Use 'pub' or 'sub'\n", *mode)
		os.Exit(1)
	}
}

func runPublisher(ctx *quicmq.Context, address string, interval time.Duration, sigChan chan os.Signal) {
	fmt.Printf("Starting publisher on %s\n", address)
	fmt.Printf("Publishing messages every %v\n", interval)

	// Create publisher socket
	socket, err := ctx.NewSocket(quicmq.PUB)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create socket: %v\n", err)
		os.Exit(1)
	}
	defer socket.Close()

	// Bind to address
	if err := socket.Bind(address); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to bind to %s: %v\n", address, err)
		os.Exit(1)
	}

	fmt.Println("Publisher ready, waiting for subscribers...")
	time.Sleep(2 * time.Second) // Give subscribers time to connect

	// Seed random number generator
	rand.Seed(time.Now().UnixNano())

	// Message counter
	msgCount := 0

	// Publishing loop
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			msgCount++

			// Generate random data
			randomNum := rand.Intn(1000)
			temperature := 20.0 + rand.Float64()*15.0 // 20-35°C

			message := fmt.Sprintf(
				"Message #%d | Random: %d | Temp: %.2f°C | Time: %s",
				msgCount,
				randomNum,
				temperature,
				time.Now().Format("15:04:05"),
			)

			// Send message
			if err := socket.Send([]byte(message), 0); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to send message: %v\n", err)
			} else {
				fmt.Printf("📤 Sent: %s\n", message)
			}

		case <-sigChan:
			fmt.Println("\n🛑 Shutting down publisher...")
			return
		}
	}
}

func runSubscriber(ctx *quicmq.Context, address string, sigChan chan os.Signal) {
	fmt.Printf("Starting subscriber connecting to %s\n", address)

	// Create subscriber socket
	socket, err := ctx.NewSocket(quicmq.SUB)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create socket: %v\n", err)
		os.Exit(1)
	}
	defer socket.Close()

	// Connect to publisher
	if err := socket.Connect(address); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to %s: %v\n", address, err)
		os.Exit(1)
	}

	fmt.Println("Subscriber connected, waiting for messages...")

	// Message counter
	msgCount := 0

	// Receiving loop
	go func() {
		for {
			msg, err := socket.Recv(0)
			if err != nil {
				// Check if it's because socket was closed
				select {
				case <-sigChan:
					return
				default:
					fmt.Fprintf(os.Stderr, "Failed to receive message: %v\n", err)
					time.Sleep(100 * time.Millisecond)
					continue
				}
			}

			msgCount++
			fmt.Printf("📥 Received (#%d): %s\n", msgCount, string(msg))
		}
	}()

	// Wait for shutdown signal
	<-sigChan
	fmt.Println("\n🛑 Shutting down subscriber...")
}
