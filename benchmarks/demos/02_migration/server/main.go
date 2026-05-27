// Demo 2 — Connection Migration Server
//
// A QUIC REP server that echoes requests and logs the remote address of each
// request. This lets you watch on the server side as the client's source port
// changes during migration — and confirm requests keep arriving without
// interruption.
//
// ── SETUP ─────────────────────────────────────────────────────────────────
// Run this on the Raspberry Pi.
//
// Build for Pi (ARM64) from the repo root on your laptop:
//
//	GOARCH=arm64 GOOS=linux go build -o demo2_server ./benchmarks/demos/02_migration/server
//	scp demo2_server pi@<PI_IP>:~/
//
// ENV VARS (all optional):
//
//	QUIC_PORT   UDP port to listen on  (default: 7003)
//
// FIREWALL:
//	sudo ufw allow 7003/udp
//
// Leave this running. Start demo2_client on the laptop.
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
	port := envOrDefault("QUIC_PORT", "7003")

	banner("QuicMQ Demo 2 · Connection Migration Server")
	info("QUIC REP on UDP :%s — logging client addresses", port)
	info("Watch how the client source port changes after migration,")
	info("while requests keep arriving without interruption.\n")

	ctx := context.Background()
	rep := quicmq.NewRep(ctx)
	if err := rep.Listen("quic://0.0.0.0:" + port); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: listen: %v\n", err)
		os.Exit(1)
	}
	info("Listening. Waiting for client...\n")

	reqNum := 0
	lastAddr := ""

	for {
		msg, err := rep.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			fmt.Fprintf(os.Stderr, " recv error: %v\n", err)
			continue
		}

		reqNum++
		payload := string(msg.Frames[0])

		// Extract the request number the client embedded, and the client's
		// current local address (which the client sends as the second frame).
		clientAddr := ""
		if len(msg.Frames) >= 2 {
			clientAddr = string(msg.Frames[1])
		}

		migrated := ""
		if clientAddr != lastAddr && lastAddr != "" {
			migrated = "\033[1;33m ← PATH CHANGED\033[0m"
		}
		if lastAddr == "" && clientAddr != "" {
			migrated = "\033[1;32m ← first contact\033[0m"
		}
		lastAddr = clientAddr

		fmt.Printf(" \033[36m[req #%02d]\033[0m  %-20s  client: \033[1m%s\033[0m%s\n",
			reqNum, payload, clientAddr, migrated)

		reply := quicmq.NewMsgFrom(
			[]byte(fmt.Sprintf("pong-%d", reqNum)),
			[]byte(clientAddr),
		)
		if err := rep.Send(reply); err != nil {
			fmt.Fprintf(os.Stderr, " send error: %v\n", err)
		}
	}
}
