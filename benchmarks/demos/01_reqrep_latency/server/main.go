// Demo 1 — REQ/REP Latency Server
//
// Runs side-by-side QUIC and TCP REP servers so the client can measure
// tail-latency under the same workload for both transports.
//
// ── SETUP ─────────────────────────────────────────────────────────────────
// Run this binary on the Raspberry Pi.
//
// Build for Pi (ARM64) from the repo root on your laptop:
//
//	GOARCH=arm64 GOOS=linux go build -o demo1_server ./benchmarks/demos/01_reqrep_latency/server
//	scp demo1_server pi@<PI_IP>:~/
//
// Or if Go is installed on the Pi, run directly:
//
//	go run ./benchmarks/demos/01_reqrep_latency/server
//
// ENV VARS (all optional):
//
//	QUIC_PORT  UDP port for the QUIC REP server  (default: 7001)
//	TCP_PORT   TCP port for the TCP REP server   (default: 7002)
//
// FIREWALL: make sure the Pi allows inbound UDP:7001 and TCP:7002.
//
//	sudo ufw allow 7001/udp
//	sudo ufw allow 7002/tcp
//
// Leave this running. Start the client on the laptop.
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

// ── TUI helpers ──────────────────────────────────────────────────────────

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

func event(color, label, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	fmt.Printf(" \033[%sm[%s]\033[0m %s\n", color, label, msg)
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// ── server loop ──────────────────────────────────────────────────────────

func serveRep(ctx context.Context, transport, addr, label string) {
	rep := quicmq.NewRep(ctx)
	if err := rep.Listen(transport + "://" + addr); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: %s Listen %s://%s: %v\n", label, transport, addr, err)
		os.Exit(1)
	}
	event("32", label, "listening on %s://%s — echoing requests", transport, addr)

	for {
		msg, err := rep.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			event("31", label, "recv error: %v", err)
			continue
		}
		if err := rep.Send(msg); err != nil {
			event("31", label, "send error: %v", err)
		}
	}
}

func main() {
	quicPort := envOrDefault("QUIC_PORT", "7001")
	tcpPort := envOrDefault("TCP_PORT", "7002")

	banner("QuicMQ Demo 1 · REQ/REP Latency Server")
	info("QUIC REP  →  UDP :%s  (TLS 1.3)", quicPort)
	info("TCP  REP  →  TCP :%s  (NULL mechanism)", tcpPort)
	info("Leave this running and start demo1_client on the laptop.\n")

	ctx := context.Background()

	go serveRep(ctx, "quic", "0.0.0.0:"+quicPort, "QUIC")
	serveRep(ctx, "tcp", "0.0.0.0:"+tcpPort, "TCP")
}
