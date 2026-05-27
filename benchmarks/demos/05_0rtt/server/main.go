// Demo 5 — 0-RTT Session Resumption Server
//
// Runs two REP servers concurrently:
//   - QUIC on QUIC_PORT (default 7009)
//   - TCP  on TCP_PORT  (default 7010)
//
// Each server echoes the request back, printing how long it took to receive
// the first frame. This lets the client measure round-trip times cleanly.
//
// ── SETUP ─────────────────────────────────────────────────────────────────
// 1. Run this on the Raspberry Pi first.
// 2. Run demo5_client on the laptop.
//
// Build:
//   go build -o demo5_server ./benchmarks/demos/05_0rtt/server
//
// Run:
//   ./demo5_server
//
// ENV VARS:
//   QUIC_PORT   QUIC REP port  (default: 7009)
//   TCP_PORT    TCP REP port   (default: 7010)
// ──────────────────────────────────────────────────────────────────────────

package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"quicmq"
)

const bannerW = 64

func info(format string, args ...any) {
	fmt.Printf(" \033[36m·\033[0m "+format+"\n", args...)
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func serveREP(ctx context.Context, transport, addr string) {
	rep := quicmq.NewRep(ctx)
	defer rep.Close()

	endpoint := transport + "://" + addr
	if err := rep.Listen(endpoint); err != nil {
		fmt.Fprintf(os.Stderr, "[%s] Listen failed: %v\n", strings.ToUpper(transport), err)
		return
	}

	color := "36"
	if transport == "tcp" {
		color = "33"
	}
	fmt.Printf(" \033[%sm●\033[0m  %s REP listening on %s\n", color, strings.ToUpper(transport), endpoint)

	n := 0
	for {
		msg, err := rep.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			fmt.Printf(" [%s] recv error: %v\n", strings.ToUpper(transport), err)
			time.Sleep(50 * time.Millisecond)
			continue
		}
		n++
		payload := ""
		if len(msg.Frames) > 0 {
			payload = string(msg.Frames[0])
		}
		fmt.Printf(" \033[%sm[%s]\033[0m  #%03d  %s\n",
			color, strings.ToUpper(transport), n, payload)

		if err := rep.Send(quicmq.NewMsg(msg.Frames[0])); err != nil {
			fmt.Printf(" [%s] send error: %v\n", strings.ToUpper(transport), err)
		}
	}
}

func main() {
	quicPort := envOrDefault("QUIC_PORT", "7009")
	tcpPort := envOrDefault("TCP_PORT", "7010")

	border := strings.Repeat("═", bannerW)
	title := "QuicMQ Demo 5 · 0-RTT Session Resumption"
	pad := (bannerW - len(title)) / 2
	right := bannerW - pad - len(title)
	fmt.Printf("\n\033[1;34m╔%s╗\033[0m\n", border)
	fmt.Printf("\033[1;34m║\033[0m\033[1m%s%s%s\033[0m\033[1;34m║\033[0m\n",
		strings.Repeat(" ", pad), title, strings.Repeat(" ", right))
	fmt.Printf("\033[1;34m╚%s╝\033[0m\n\n", border)

	info("QUIC REP  →  UDP  0.0.0.0:%s", quicPort)
	info("TCP REP   →  TCP  0.0.0.0:%s", tcpPort)
	info("Echoes all requests. Shows transport + sequence number.")
	fmt.Println()

	ctx := context.Background()
	go serveREP(ctx, "quic", "0.0.0.0:"+quicPort)
	go serveREP(ctx, "tcp", "0.0.0.0:"+tcpPort)

	// Block forever
	select {}
}
