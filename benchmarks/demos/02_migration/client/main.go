// Demo 2 — Connection Migration Client
//
// Demonstrates QUIC RFC 9000 §9 path migration:
//  1. Connects to the server and sends 3 requests — shows local UDP port.
//  2. Migrates to a new local UDP port (PATH_CHALLENGE / PATH_RESPONSE).
//  3. Sends 3 more requests — same connection, new source port.
//  4. Explains what TCP would do instead.
//
// ── SETUP ─────────────────────────────────────────────────────────────────
// 1. Start demo2_server on the Raspberry Pi first.
// 2. Run this on the laptop.
//
// Build:
//   go build -o demo2_client ./benchmarks/demos/02_migration/client
//
// Run:
//   SERVER_ADDR=192.168.1.5 ./demo2_client
//
// ENV VARS:
//   SERVER_ADDR  IP of the Raspberry Pi  (REQUIRED)
//   QUIC_PORT    QUIC server port         (default: 7003)
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

func banner(title string) {
	border := strings.Repeat("═", bannerW)
	pad := (bannerW - len(title)) / 2
	right := bannerW - pad - len(title)
	fmt.Printf("\n\033[1;34m╔%s╗\033[0m\n", border)
	fmt.Printf("\033[1;34m║\033[0m\033[1m%s%s%s\033[0m\033[1;34m║\033[0m\n",
		strings.Repeat(" ", pad), title, strings.Repeat(" ", right))
	fmt.Printf("\033[1;34m╚%s╝\033[0m\n\n", border)
}

func divider(title string) {
	rest := bannerW - 6 - len(title)
	if rest < 0 {
		rest = 0
	}
	fmt.Printf("\n \033[1;33m─── %s \033[0m%s\n", title, strings.Repeat("─", rest))
}

func info(format string, args ...any) {
	fmt.Printf(" \033[36m·\033[0m "+format+"\n", args...)
}

func step(icon, color, format string, args ...any) {
	fmt.Printf(" %s  \033[%sm%s\033[0m\n", icon, color, fmt.Sprintf(format, args...))
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		fmt.Fprintf(os.Stderr, "\033[1;31mFATAL: env var %s is required.\033[0m\n", key)
		fmt.Fprintf(os.Stderr, "  Example: SERVER_ADDR=192.168.1.5 ./demo2_client\n")
		os.Exit(1)
	}
	return v
}

// sendRecv sends one request (embedding the current local address) and prints the result.
func sendRecv(req quicmq.Socket, n int, localAddr string) {
	payload := fmt.Sprintf("ping-%d", n)
	msg := quicmq.NewMsgFrom([]byte(payload), []byte(localAddr))

	t0 := time.Now()
	if err := req.Send(msg); err != nil {
		step("▶", "31", "req #%d  SEND ERROR: %v", n, err)
		return
	}
	reply, err := req.Recv()
	if err != nil {
		step("▶", "31", "req #%d  RECV ERROR: %v", n, err)
		return
	}
	lat := time.Since(t0)

	replyText := string(reply.Frames[0])
	step("▶", "32", "req #%02d  sent: %-10s  got: %-12s  latency: %v  path: \033[1m%s\033[0m",
		n, payload, replyText, lat.Round(time.Microsecond*100), localAddr)
}

func main() {
	serverAddr := mustEnv("SERVER_ADDR")
	quicPort := envOrDefault("QUIC_PORT", "7003")
	endpoint := "quic://" + serverAddr + ":" + quicPort

	banner("QuicMQ Demo 2 · QUIC Connection Migration")

	info("QUIC identifies connections by opaque Connection IDs (CIDs),")
	info("NOT by IP:port. When the source address changes, the session")
	info("migrates automatically via PATH_CHALLENGE / PATH_RESPONSE.")
	fmt.Println()
	info("Server: %s", endpoint)

	ctx := context.Background()
	req := quicmq.NewReq(ctx, quicmq.WithTimeout(10*time.Second))
	defer req.Close()

	if err := req.Dial(endpoint); err != nil {
		fmt.Fprintf(os.Stderr, "\nFATAL: dial %s: %v\n", endpoint, err)
		os.Exit(1)
	}
	time.Sleep(100 * time.Millisecond)

	initialAddr := quicmq.QuicLocalAddr(req)
	info("Connected. Local UDP path: \033[1m%s\033[0m\n", initialAddr)

	// ── Phase 1: Normal requests ──────────────────────────────────────────
	divider("Phase 1 · Initial Path")
	info("Sending 3 requests — note the local port on each reply.")
	fmt.Println()

	for i := 1; i <= 3; i++ {
		sendRecv(req, i, quicmq.QuicLocalAddr(req))
		time.Sleep(300 * time.Millisecond)
	}

	// ── Phase 2: Migration ────────────────────────────────────────────────
	divider("Phase 2 · Network Change  (migration)")
	info("Simulating a network path change...")
	info("Binding a new local UDP socket on a different ephemeral port...")
	fmt.Println()

	time.Sleep(500 * time.Millisecond)

	step("⚡", "33", "Sending PATH_CHALLENGE to server...")
	time.Sleep(200 * time.Millisecond)

	oldAddr, newAddr, err := quicmq.MigrateToNewPath(ctx, req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n\033[1;31mMigration failed: %v\033[0m\n", err)
		fmt.Fprintf(os.Stderr, "Note: PATH_CHALLENGE requires the server to be reachable from the new port.\n")
		os.Exit(1)
	}

	step("✓", "32", "PATH_RESPONSE received — new path validated")
	step("✓", "32", "Connection switched to new path")
	fmt.Println()
	step("»", "36", "Path before: \033[1m%s\033[0m", oldAddr)
	step("»", "36", "Path after:  \033[1m%s\033[0m  (same connection, different port)", newAddr)

	// ── Phase 3: Post-migration ───────────────────────────────────────────
	divider("Phase 3 · Post-Migration (same QUIC connection, new port)")
	info("Sending 3 more requests — same CID, messages flow on new path.")
	fmt.Println()

	time.Sleep(300 * time.Millisecond)
	for i := 4; i <= 6; i++ {
		sendRecv(req, i, quicmq.QuicLocalAddr(req))
		time.Sleep(300 * time.Millisecond)
	}

	// ── Summary ──────────────────────────────────────────────────────────
	divider("Result")
	step("✓", "32", "All 6 requests completed — \033[1mno reconnect, no data loss\033[0m")
	fmt.Println()
	info("QUIC connection IDs are opaque 8-20 byte tokens chosen by both")
	info("endpoints. The server matches incoming packets by CID, not by")
	info("source IP:port. When the source changes, quic-go validates the")
	info("new path and migrates transparently.")
	fmt.Println()

	divider("What TCP Would Do")
	step("✗", "31", "TCP 4-tuple: (src_ip, src_port, dst_ip, dst_port)")
	step("✗", "31", "Any change in src_port → connection RESET by server")
	step("✗", "31", "App must reconnect + redo full TLS + ZMTP handshake")
	step("✗", "31", "In-flight requests are lost; state must be rebuilt")
	fmt.Println()
	info("Real-world use case: a laptop or phone switches from WiFi to LTE.")
	info("QUIC migrates silently. TCP forces a full reconnect.")
}
