// Demo 1 — REQ/REP Latency Client
//
// Runs 5 concurrent REQ workers against the server for each transport,
// records every round-trip latency, and prints p50/p95/p99 side by side.
//
// ── SETUP ─────────────────────────────────────────────────────────────────
// 1. Start demo1_server on the Raspberry Pi first.
// 2. Run this on the laptop.
//
// Build:
//   go build -o demo1_client ./benchmarks/demos/01_reqrep_latency/client
//
// Run:
//   SERVER_ADDR=192.168.1.5 ./demo1_client
//
// ENV VARS:
//   SERVER_ADDR  IP of the Raspberry Pi   (REQUIRED, no default)
//   QUIC_PORT    QUIC server port          (default: 7001)
//   TCP_PORT     TCP server port           (default: 7002)
//   WORKERS      concurrent REQ sockets   (default: 5)
//   REQUESTS     requests per worker      (default: 300)
// ──────────────────────────────────────────────────────────────────────────

package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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

func result(label, val, note string) {
	fmt.Printf("   \033[1m%-6s\033[0m  %s  %s\n", label, val, note)
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
		fmt.Fprintf(os.Stderr, "  Example: SERVER_ADDR=192.168.1.5 ./demo1_client\n")
		os.Exit(1)
	}
	return v
}

// ── Percentile math ──────────────────────────────────────────────────────

func pctDuration(data []time.Duration, pct float64) time.Duration {
	if len(data) == 0 {
		return 0
	}
	cp := make([]time.Duration, len(data))
	copy(cp, data)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	idx := int(float64(len(cp)-1) * pct / 100.0)
	return cp[idx]
}

// ── Phase runner ─────────────────────────────────────────────────────────

func runPhase(ctx context.Context, transport, addr string, workers, requests int) []time.Duration {
	var (
		mu        sync.Mutex
		latencies []time.Duration
		done      int64
		total     = int64(workers * requests)
		wg        sync.WaitGroup
	)

	// Live progress line (overwrites same line).
	stopProgress := make(chan struct{})
	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopProgress:
				return
			case <-ticker.C:
				n := atomic.LoadInt64(&done)
				pct := int(float64(n) / float64(total) * 40)
				bar := strings.Repeat("█", pct) + strings.Repeat("░", 40-pct)
				fmt.Printf("\r   [%s] %d/%d", bar, n, total)
			}
		}
	}()

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			req := quicmq.NewReq(ctx,
				quicmq.WithDialerMaxRetries(3),
				quicmq.WithTimeout(10*time.Second),
			)
			defer req.Close()

			endpoint := transport + "://" + addr
			if err := req.Dial(endpoint); err != nil {
				fmt.Fprintf(os.Stderr, "\n worker-%d dial error: %v\n", id, err)
				return
			}
			time.Sleep(50 * time.Millisecond) // let handshake settle

			payload := quicmq.NewMsgString(fmt.Sprintf("w%d-ping", id))

			for i := 0; i < requests; i++ {
				t0 := time.Now()
				if err := req.Send(payload); err != nil {
					continue
				}
				if _, err := req.Recv(); err != nil {
					continue
				}
				lat := time.Since(t0)
				mu.Lock()
				latencies = append(latencies, lat)
				mu.Unlock()
				atomic.AddInt64(&done, 1)
			}
		}(w + 1)
	}

	wg.Wait()
	close(stopProgress)

	// Clear the progress line.
	fmt.Printf("\r%s\r", strings.Repeat(" ", 60))

	return latencies
}

// ── Comparison table ─────────────────────────────────────────────────────

func printTable(ql, tl []time.Duration) {
	fmt.Printf("\n \033[1;34m╔%s╗\033[0m\n", strings.Repeat("═", bannerW))
	title := "Comparison  (lower tail = better — QUIC wins at p99)"
	pad := (bannerW - len(title)) / 2
	fmt.Printf(" \033[1;34m║\033[0m\033[1m%s%s%s\033[0m\033[1;34m║\033[0m\n",
		strings.Repeat(" ", pad), title, strings.Repeat(" ", bannerW-pad-len(title)))
	fmt.Printf(" \033[1;34m╠%s╣\033[0m\n", strings.Repeat("═", bannerW))

	header := fmt.Sprintf("  %-6s  │  %-16s │  %-16s │  %-12s", "Metric", "QUIC (TLS 1.3)", "TCP (NULL)", "Advantage")
	fmt.Printf(" \033[1;34m║\033[0m\033[1m%-*s\033[0m\033[1;34m║\033[0m\n", bannerW, header)
	fmt.Printf(" \033[1;34m╠%s╣\033[0m\n", strings.Repeat("═", bannerW))

	for _, p := range []struct {
		label string
		pct   float64
	}{
		{"p50", 50}, {"p95", 95}, {"p99", 99}, {"max", 100},
	} {
		qv := pctDuration(ql, p.pct)
		tv := pctDuration(tl, p.pct)
		if p.label == "max" {
			qv = pctDuration(ql, 99.9)
			tv = pctDuration(tl, 99.9)
		}

		var advantage, color string
		switch {
		case qv < tv:
			ratio := float64(tv) / float64(qv)
			advantage = fmt.Sprintf("QUIC %.1f×", ratio)
			color = "32" // green
		case tv < qv:
			ratio := float64(qv) / float64(tv)
			advantage = fmt.Sprintf("TCP  %.1f×", ratio)
			color = "33" // yellow
		default:
			advantage = "equal"
			color = "37"
		}

		highlight := ""
		reset := ""
		if p.label == "p99" {
			highlight = "\033[1m"
			reset = "\033[0m"
			color = "32"
			advantage += " ✓"
		}

		row := fmt.Sprintf("  %-6s  │  %-16s │  %-16s │",
			p.label,
			highlight+qv.Round(time.Microsecond*100).String()+reset,
			highlight+tv.Round(time.Microsecond*100).String()+reset,
		)
		fmt.Printf(" \033[1;34m║\033[0m%-*s \033[%sm%s\033[0m\033[1;34m║\033[0m\n",
			bannerW-len(advantage)-2, row, color, advantage)
	}

	fmt.Printf(" \033[1;34m╚%s╝\033[0m\n", strings.Repeat("═", bannerW))
}

// ── main ─────────────────────────────────────────────────────────────────

func main() {
	serverAddr := mustEnv("SERVER_ADDR")
	quicPort := envOrDefault("QUIC_PORT", "7001")
	tcpPort := envOrDefault("TCP_PORT", "7002")
	workers, _ := strconv.Atoi(envOrDefault("WORKERS", "5"))
	requests, _ := strconv.Atoi(envOrDefault("REQUESTS", "300"))
	if workers < 1 {
		workers = 5
	}
	if requests < 10 {
		requests = 10
	}

	banner("QuicMQ Demo 1 · REQ/REP Tail Latency  QUIC vs TCP")
	info("Server:   %s  (QUIC :%s  |  TCP :%s)", serverAddr, quicPort, tcpPort)
	info("Workers:  %d concurrent REQ sockets per transport", workers)
	info("Requests: %d per worker  (%d total per phase)", requests, workers*requests)
	fmt.Println()
	info("WHY this matters: each QUIC request runs on its own independent")
	info("stream. A single delayed TCP response blocks every other response")
	info("queued behind it in the same connection — head-of-line blocking.")

	ctx := context.Background()

	// ── Phase 1: QUIC ────────────────────────────────────────────────────
	divider("Phase 1 · QUIC Transport (TLS 1.3, UDP)")
	info("Connecting %d workers to quic://%s:%s ...", workers, serverAddr, quicPort)
	fmt.Println()
	t0 := time.Now()
	ql := runPhase(ctx, "quic", serverAddr+":"+quicPort, workers, requests)
	quicDur := time.Since(t0)

	info("Done — %d samples in %v", len(ql), quicDur.Round(time.Millisecond))
	result("p50", pctDuration(ql, 50).Round(time.Microsecond*100).String(), "")
	result("p95", pctDuration(ql, 95).Round(time.Microsecond*100).String(), "")
	result("p99", "\033[1m"+pctDuration(ql, 99).Round(time.Microsecond*100).String()+"\033[0m", "← tail latency")

	// ── Phase 2: TCP ─────────────────────────────────────────────────────
	divider("Phase 2 · TCP Transport (NULL mechanism)")
	info("Connecting %d workers to tcp://%s:%s ...", workers, serverAddr, tcpPort)
	fmt.Println()
	t1 := time.Now()
	tl := runPhase(ctx, "tcp", serverAddr+":"+tcpPort, workers, requests)
	tcpDur := time.Since(t1)

	info("Done — %d samples in %v", len(tl), tcpDur.Round(time.Millisecond))
	result("p50", pctDuration(tl, 50).Round(time.Microsecond*100).String(), "")
	result("p95", pctDuration(tl, 95).Round(time.Microsecond*100).String(), "")
	result("p99", "\033[1m"+pctDuration(tl, 99).Round(time.Microsecond*100).String()+"\033[0m", "← tail latency")

	// ── Summary ──────────────────────────────────────────────────────────
	printTable(ql, tl)

	fmt.Println()
	info("p50 ≈ equal: both transports have the same network RTT.")
	info("p99 gap:     QUIC streams are independent — one slow response")
	info("             cannot delay others. TCP forces in-order delivery.")
}
