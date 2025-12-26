package main

import (
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"time"

	quicmq "quicmq/lib"
)

func main() {
	// Parse command line arguments
	mode := flag.String("m", "pub", "Mode: pub (publisher) or sub (subscriber)")
	address := flag.String("a", "quic://127.0.0.1:5000", "Address to bind/connect (e.g., quic://127.0.0.1:5000)")
	interval := flag.Duration("i", 1, "Interval between messages (publisher only)")
	topic := flag.String("t", "RAND", "Topic to subscriber to")
	flag.Parse()

	// Setup logger
	opts := &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, opts))
	slog.SetDefault(logger)

	// setup context
	quicContext, err := quicmq.NewQuicContext()
	if err != nil {
		slog.Error("Failed to create QUIC context", "error", err)
		os.Exit(1)
	}
	defer quicContext.Close()

	var socket quicmq.Socket
	switch *mode {
	case "pub":
		socket, err = quicContext.NewSocket(quicmq.PUB, quicmq.WithBind(*address))
		if err != nil {
			slog.Error("Failed to create publisher socket", "error", err)
			os.Exit(1)
		}
		slog.Info("Publisher started...")
		for {
			msg := fmt.Sprintf("%s:%d\n", *topic, rand.Int())
			socket.Send([]byte(msg))
			time.Sleep(*interval * time.Second)
		}
	case "sub":
		socket, err = quicContext.NewSocket(quicmq.SUB, quicmq.WithConnect(*address))
		if err != nil {
			slog.Error("Failed to create subscriber socket", "error", err)
			os.Exit(1)
		}
		slog.Info("Subscriber started...")
		socket.Subscribe(*topic)
		slog.Info("Subscribed to topic", "topic", *topic)
		for {
			msg, err := socket.Recv()
			if err != nil {
				slog.Error("Failed to receive message", "error", err)
			}
			slog.Info("Received", "msg", string(msg))
		}
	}
}
