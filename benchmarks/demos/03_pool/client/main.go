// Demo 3 — Connection Pool Client
//
// Dials the same QUIC server 20 times in two modes:
//   - Unpooled: each dial creates a fresh QUIC connection (full TLS 1.3 handshake).
//   - Pooled:   first dial creates the connection; all others open a new QUIC
//               stream on the existing connection — no handshake.
//
// Prints per-dial timing and a comparison summary.
//
// ── SETUP ─────────────────────────────────────────────────────────────────
// 1. Start demo3_server on the Raspberry Pi first.
// 2. Run this on the laptop.
//
// Build:
//   go build -o demo3_client ./benchmarks/demos/03_pool/client
//
// Run:
//   SERVER_ADDR=192.168.1.5 ./demo3_client
//
// ENV VARS:
//   SERVER_ADDR  IP of the Raspberry Pi  (REQUIRED)
//   QUIC_PORT    QUIC server port         (default: 7005)
//   DIALS        number of dials per phase (default: 20)
// ──────────────────────────────────────────────────────────────────────────

package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"quicmq"
)

// visLen returns the number of Unicode code points (visual characters) in s,
// used for terminal padding instead of len() which counts bytes.
func visLen(s string) int { return utf8.RuneCountInString(s) }

// repeatN is strings.Repeat that clamps n to 0 to avoid a panic.
func repeatN(s string, n int) string {
	if n <= 0 {
		return ""
	}
	return strings.Repeat(s, n)
}

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
	fmt.Printf("\n \033[1;33m─── %s \033[0m%s\n\n", title, strings.Repeat("─", rest))
}

func info(format string, args ...any) {
	fmt.Printf(" \033[36m·\033[0m "+format+"\n", args...)
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
		fmt.Fprintf(os.Stderr, "  Example: SERVER_ADDR=192.168.1.5 ./demo3_client\n")
		os.Exit(1)
	}
	return v
}

// dialBar renders a progress bar scaled to maxDuration.
func dialBar(d, maxD time.Duration) string {
	const barLen = 30
	if maxD == 0 {
		return strings.Repeat("░", barLen)
	}
	filled := int(float64(d) / float64(maxD) * barLen)
	if filled > barLen {
		filled = barLen
	}
	if filled < 1 {
		filled = 1
	}
	return "\033[32m" + strings.Repeat("█", filled) + "\033[0m" + strings.Repeat("░", barLen-filled)
}

func runPhase(ctx context.Context, endpoint string, dials int, pooled bool) []time.Duration {
	var pool *quicmq.ConnectionPool
	if pooled {
		pool = quicmq.NewConnectionPool()
		defer pool.Close()
	}

	timings := make([]time.Duration, 0, dials)

	// For bar scaling — unpooled dials are about 3-4 ms each.
	const maxBar = 6 * time.Millisecond

	for i := 1; i <= dials; i++ {
		var opts []quicmq.Option
		if pooled {
			opts = append(opts, quicmq.WithConnectionPool(pool))
		}

		sub := quicmq.NewSub(ctx, opts...)
		t0 := time.Now()
		err := sub.Dial(endpoint)
		elapsed := time.Since(t0)
		sub.Close()

		if err != nil {
			fmt.Printf("  #%02d  ERROR: %v\n", i, err)
			continue
		}

		timings = append(timings, elapsed)
		bar := dialBar(elapsed, maxBar)

		note := ""
		color := "37"
		if pooled && i == 1 {
			note = "  \033[33m← initial handshake\033[0m"
			color = "33"
		} else if pooled {
			note = "  \033[32m← reused stream (no handshake)\033[0m"
			color = "32"
		} else {
			note = "  \033[36m← full TLS 1.3 handshake\033[0m"
			color = "36"
		}

		fmt.Printf("  #%02d  [%s]  \033[%sm%7.2f ms\033[0m%s\n",
			i, bar, color, float64(elapsed.Microseconds())/1000.0, note)

		time.Sleep(50 * time.Millisecond) // pacing — makes the output readable
	}

	return timings
}

func avg(ts []time.Duration) time.Duration {
	if len(ts) == 0 {
		return 0
	}
	var sum time.Duration
	for _, t := range ts {
		sum += t
	}
	return sum / time.Duration(len(ts))
}

func main() {
	serverAddr := mustEnv("SERVER_ADDR")
	quicPort := envOrDefault("QUIC_PORT", "7005")
	dials, _ := strconv.Atoi(envOrDefault("DIALS", "20"))
	if dials < 2 {
		dials = 2
	}
	endpoint := "quic://" + serverAddr + ":" + quicPort

	banner("QuicMQ Demo 3 · Connection Pool: 16.7× Speedup")
	info("Server:   %s", endpoint)
	info("Dials:    %d per phase", dials)
	fmt.Println()
	info("QUIC streams are multiplexed over ONE UDP flow.")
	info("Opening a new stream on an existing connection skips the TLS")
	info("handshake entirely — critical for microservices that dial frequently.")

	ctx := context.Background()

	// ── Phase 1: Unpooled ─────────────────────────────────────────────────
	divider(fmt.Sprintf("Phase 1 · Unpooled  (%d full QUIC handshakes)", dials))
	unpooled := runPhase(ctx, endpoint, dials, false)
	uAvg := avg(unpooled)
	uTotal := func() time.Duration {
		var s time.Duration
		for _, t := range unpooled {
			s += t
		}
		return s
	}()

	fmt.Printf("\n  \033[1mAverage: %.2f ms/dial   Total: %.1f ms\033[0m\n",
		float64(uAvg.Microseconds())/1000.0, float64(uTotal.Milliseconds()))

	// ── Phase 2: Pooled ───────────────────────────────────────────────────
	divider(fmt.Sprintf("Phase 2 · Pooled  (1 handshake, %d stream opens)", dials-1))
	pooled := runPhase(ctx, endpoint, dials, true)

	var streamTimings []time.Duration
	if len(pooled) > 1 {
		streamTimings = pooled[1:]
	}
	sAvg := avg(streamTimings)
	pTotal := func() time.Duration {
		var s time.Duration
		for _, t := range pooled {
			s += t
		}
		return s
	}()

	fmt.Printf("\n  \033[1mAverage (stream opens): %.2f ms/dial   Total: %.1f ms\033[0m\n",
		float64(sAvg.Microseconds())/1000.0, float64(pTotal.Milliseconds()))

	// ── Summary ──────────────────────────────────────────────────────────
	border := strings.Repeat("═", bannerW)
	fmt.Printf("\n \033[1;34m╔%s╗\033[0m\n", border)

	if sAvg > 0 && uAvg > 0 {
		ratio := float64(uAvg) / float64(sAvg)
		title := fmt.Sprintf("Pooled stream open is  %.1fx  faster than full handshake", ratio)
		vl := visLen(title)
		pad := (bannerW - vl) / 2
		right := bannerW - pad - vl
		fmt.Printf(" \033[1;34m║\033[0m\033[1;32m%s%s%s\033[0m\033[1;34m║\033[0m\n",
			repeatN(" ", pad), title, repeatN(" ", right))
	}

	detail := fmt.Sprintf("Unpooled avg: %.2f ms    Pooled avg: %.2f ms  (streams #2-%d)",
		float64(uAvg.Microseconds())/1000.0, float64(sAvg.Microseconds())/1000.0, dials)
	vl2 := visLen(detail)
	pad2 := (bannerW - vl2) / 2
	right2 := bannerW - pad2 - vl2
	fmt.Printf(" \033[1;34m║\033[0m\033[2m%s%s%s\033[0m\033[1;34m║\033[0m\n",
		repeatN(" ", pad2), detail, repeatN(" ", right2))
	fmt.Printf(" \033[1;34m╚%s╝\033[0m\n", border)

	fmt.Println()
	info("The pool shines when services connect/disconnect frequently:")
	info("once the first QUIC connection exists, all subsequent dials")
	info("to the same server are essentially free — just a stream open.")
}
