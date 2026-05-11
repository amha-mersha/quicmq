package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"quicmq"
)

func main() {
	addr := flag.String("addr", "quic://127.0.0.1:9000", "Address to connect to")
	connectTimeout := flag.Duration("connect-timeout", 30*time.Second, "How long to keep retrying before giving up on the initial connection")
	retry := flag.Duration("retry", 250*time.Millisecond, "Delay between connection attempts")
	flag.Parse()
	printLocalAddrs()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// libzmq-style auto-reconnect: keep trying on connection loss with a
	// bounded backoff. WithDialTimeout puts a wall-clock budget on the
	// INITIAL Dial (so the subscriber exits if the publisher never comes
	// up). WithAutomaticReconnect handles in-flight disconnects.
	sub := quicmq.NewSub(ctx,
		quicmq.WithDialerRetry(*retry),
		quicmq.WithDialerMaxRetries(-1), // unlimited per-attempt retries within the budget
		quicmq.WithDialTimeout(*connectTimeout),
		quicmq.WithAutomaticReconnect(true),
	)
	defer sub.Close()

	fmt.Printf("Connecting to %s (timeout=%s)...\n", *addr, *connectTimeout)
	if err := sub.Dial(*addr); err != nil {
		log.Fatalf("dial: %v", err)
	}
	fmt.Printf("Subscriber connected to %s\n", *addr)

	if err := sub.SetOption(quicmq.OptionSubscribe, "weather"); err != nil {
		log.Fatalf("subscribe: %v", err)
	}
	fmt.Println("Subscribed to topic: weather")

	for {
		msg, err := sub.Recv()
		if err != nil {
			if isTransientErr(err) {
				// Connection dropped — the library is already retrying
				// in the background. Loop and wait for the next message.
				fmt.Printf("recv: peer disconnected (%v); waiting for reconnect...\n", err)
				time.Sleep(*retry)
				continue
			}
			log.Fatalf("recv: %v", err)
		}
		fmt.Printf("Received: %s\n", msg.Frames[0])
	}
}

// isTransientErr returns true for errors caused by a peer going away
// (EOF, closed connection, QUIC idle timeout). These are recoverable —
// the auto-reconnect goroutine inside quicmq will re-dial the publisher.
func isTransientErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, quicmq.ErrClosedConn) {
		return true
	}
	msg := err.Error()
	for _, needle := range []string{"EOF", "closed", "timeout", "no recent network activity", "Application error"} {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}

func printLocalAddrs() {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return
	}
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				fmt.Printf("Local IP: %s\n", ipnet.IP.String())
			}
		}
	}
}
