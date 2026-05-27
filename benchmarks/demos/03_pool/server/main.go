// Demo 3 — Connection Pool Server
//
// A simple QUIC PUB server. Its only job is to accept subscriber connections
// so the client can measure how fast each dial completes with and without a
// connection pool.
//
// ── SETUP ─────────────────────────────────────────────────────────────────
// Run this on the Raspberry Pi.
//
// Build for Pi (ARM64):
//
//	GOARCH=arm64 GOOS=linux go build -o demo3_server ./benchmarks/demos/03_pool/server
//	scp demo3_server pi@<PI_IP>:~/
//
// ENV VARS (all optional):
//
//	QUIC_PORT   UDP port to listen on  (default: 7005)
//
// FIREWALL:
//	sudo ufw allow 7005/udp
//
// Leave this running. Start demo3_client on the laptop.
// ──────────────────────────────────────────────────────────────────────────

package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"quicmq"
)

const bannerW = 64

func banner(title string) {
	border := strings.Repeat("═", bannerW)
	pad := (bannerW - len(title)) / 2
	right := bannerW - pad - len(title)
	fmt.Printf("\n\033[1;34m╔%s╗\033[0m\n", border)
	fmt.Printf("\033[1;34m║\033[0m\033[1m%s%s%s\033[0m\033[1;34m║\033[0m\n",
		strings.Repeat(" ", pad), title, strings.Repeat(" ", right))
	fmt.Printf("\033[1;34m╚%s╝\033[0m\n\n", border)
}

func info(format string, args ...any) {
	fmt.Printf(" \033[36m»\033[0m "+format+"\n", args...)
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	port := envOrDefault("QUIC_PORT", "7005")

	banner("QuicMQ Demo 3 · Connection Pool Server")
	info("QUIC PUB on UDP :%s", port)
	info("Accepts subscriber connections — no messages sent.")
	info("Counts how many connections arrive.")
	info("Leave this running and start demo3_client on the laptop.\n")

	ctx := context.Background()
	pub := quicmq.NewPub(ctx)
	if err := pub.Listen("quic://0.0.0.0:" + port); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: listen: %v\n", err)
		os.Exit(1)
	}

	info("Listening on quic://0.0.0.0:%s\n", port)

	// Keep the server alive — the PUB socket handles connections internally.
	select {}
}
