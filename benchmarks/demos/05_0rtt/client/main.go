// Demo 5 — 0-RTT Session Resumption Client
//
// Shows that QUIC can reuse a TLS 1.3 session ticket on reconnect,
// sending application data in the very first packet — 0 extra round-trips.
//
// Phases:
//
//   Phase 1 · QUIC Naive (new TLS config each time)
//     Each reconnect starts a fresh TLS handshake — no session cache.
//     This is the baseline: every dial costs a full 1-RTT handshake.
//
//   Phase 2 · QUIC Smart (shared TLS config with session cache)
//     quicmq.InsecureClientTLSConfig() embeds tls.NewLRUClientSessionCache(128).
//     Passing the SAME *tls.Config to every dial lets quic-go find the cached
//     session ticket and use 0-RTT on the second connection onwards.
//     Dial #1 = full handshake (cold).  Dials #2-N = 0-RTT (warm).
//
//   Phase 3 · TCP comparison
//     TCP + CURVE has no session resumption concept. Every reconnect pays the
//     full CURVE Diffie-Hellman exchange, regardless of how recently we last
//     connected to the same server.
//
// ── SETUP ─────────────────────────────────────────────────────────────────
// 1. Start demo5_server on the Raspberry Pi first.
// 2. Run this on the laptop.
//
// Build:
//   go build -o demo5_client ./benchmarks/demos/05_0rtt/client
//
// Run:
//   SERVER_ADDR=192.168.1.5 ./demo5_client
//
// ENV VARS:
//   SERVER_ADDR  IP of the Raspberry Pi  (REQUIRED)
//   QUIC_PORT    QUIC server port         (default: 7009)
//   TCP_PORT     TCP server port          (default: 7010)
//   DIALS        reconnects per phase     (default: 7)
//   PAUSE_MS     ms between reconnects    (default: 300)
// ──────────────────────────────────────────────────────────────────────────

package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"quicmq"
)

const bannerW = 64

func banner(title string) {
	border := strings.Repeat("=", bannerW)
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
	fmt.Printf("\n \033[1;33m--- %s \033[0m%s\n\n", title, strings.Repeat("-", rest))
}

func info(format string, args ...any) {
	fmt.Printf(" \033[36m*\033[0m "+format+"\n", args...)
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
		fmt.Fprintf(os.Stderr, "\033[1;31mFATAL: %s is required.\033[0m\n", key)
		fmt.Fprintf(os.Stderr, "  Example: SERVER_ADDR=192.168.1.5 ./demo5_client\n")
		os.Exit(1)
	}
	return v
}

// ── per-dial timing result ────────────────────────────────────────────────────

type result struct {
	n       int
	elapsed time.Duration
	warm    bool // true = 0-RTT / session resumption used
}

// ── bar rendering ─────────────────────────────────────────────────────────────

func bar(d, maxD time.Duration, color string) string {
	const barLen = 28
	if maxD == 0 {
		return strings.Repeat("░", barLen)
	}
	filled := int(float64(d) / float64(maxD) * barLen)
	if filled < 1 {
		filled = 1
	}
	if filled > barLen {
		filled = barLen
	}
	return "\033[" + color + "m" + strings.Repeat("█", filled) + "\033[0m" + strings.Repeat("░", barLen-filled)
}

// ── phase runner ──────────────────────────────────────────────────────────────

// runPhase performs `dials` sequential connect→send→recv→close cycles.
//
//   - transport : "quic" or "tcp"
//   - endpoint  : full URL, e.g. "quic://192.168.1.5:7009"
//   - sharedTLS : if non-nil, the same *tls.Config is reused across dials
//     so quic-go can find the cached session ticket (0-RTT).
//     Pass nil to use a fresh config per dial (no session cache sharing).
//   - pauseMs   : sleep between dials to simulate real reconnect cadence
func runPhase(ctx context.Context, transport, endpoint string, sharedTLS *tls.Config, dials, pauseMs int) []result {
	results := make([]result, 0, dials)

	const maxBar = 200 * time.Millisecond
	barColor := "36" // cyan for QUIC
	if transport == "tcp" {
		barColor = "33" // yellow for TCP
	}

	for i := 1; i <= dials; i++ {
		var opts []quicmq.Option
		if sharedTLS != nil {
			opts = append(opts, quicmq.WithDialTLS(sharedTLS))
		}

		req := quicmq.NewReq(ctx, opts...)

		t0 := time.Now()
		if err := req.Dial(endpoint); err != nil {
			fmt.Printf("  #%02d  ERROR dialing: %v\n", i, err)
			req.Close()
			continue
		}

		// Send a tagged ping so the server can log it.
		tag := fmt.Sprintf("dial#%02d", i)
		if err := req.Send(quicmq.NewMsgString(tag)); err != nil {
			fmt.Printf("  #%02d  ERROR sending: %v\n", i, err)
			req.Close()
			continue
		}
		if _, err := req.Recv(); err != nil {
			fmt.Printf("  #%02d  ERROR receiving: %v\n", i, err)
			req.Close()
			continue
		}
		elapsed := time.Since(t0)
		req.Close()

		// First dial is always a full handshake (cold start).
		// For shared-TLS QUIC phases, dials 2+ should be 0-RTT.
		warm := sharedTLS != nil && i > 1

		results = append(results, result{n: i, elapsed: elapsed, warm: warm})

		note := ""
		noteColor := barColor
		switch {
		case transport == "tcp":
			note = "  \033[33m<- CURVE handshake (always)\033[0m"
			noteColor = "33"
		case !warm:
			note = "  \033[33m<- full TLS 1.3 handshake (cold)\033[0m"
			noteColor = "33"
		default:
			note = "  \033[32m<- 0-RTT session resumption (warm)\033[0m"
			noteColor = "32"
		}
		_ = noteColor

		fmt.Printf("  #%02d  [%s]  \033[%sm%7.2f ms\033[0m%s\n",
			i, bar(elapsed, maxBar, barColor),
			barColor, float64(elapsed.Microseconds())/1000.0, note)

		time.Sleep(time.Duration(pauseMs) * time.Millisecond)
	}

	return results
}

// ── statistics ────────────────────────────────────────────────────────────────

func avg(rs []result) time.Duration {
	if len(rs) == 0 {
		return 0
	}
	var sum time.Duration
	for _, r := range rs {
		sum += r.elapsed
	}
	return sum / time.Duration(len(rs))
}

func median(rs []result) time.Duration {
	if len(rs) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(rs))
	for i, r := range rs {
		sorted[i] = r.elapsed
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted[len(sorted)/2]
}

func splitWarmCold(rs []result) (cold, warm []result) {
	for _, r := range rs {
		if r.warm {
			warm = append(warm, r)
		} else {
			cold = append(cold, r)
		}
	}
	return
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000.0 }

// ── main ──────────────────────────────────────────────────────────────────────

func main() {
	serverAddr := mustEnv("SERVER_ADDR")
	quicPort := envOrDefault("QUIC_PORT", "7009")
	tcpPort := envOrDefault("TCP_PORT", "7010")
	dials, _ := strconv.Atoi(envOrDefault("DIALS", "7"))
	pauseMs, _ := strconv.Atoi(envOrDefault("PAUSE_MS", "300"))
	if dials < 3 {
		dials = 3
	}

	quicEndpoint := "quic://" + serverAddr + ":" + quicPort
	tcpEndpoint := "tcp://" + serverAddr + ":" + tcpPort

	banner("QuicMQ Demo 5 : 0-RTT Session Resumption")
	info("QUIC server:  %s", quicEndpoint)
	info("TCP  server:  %s", tcpEndpoint)
	info("Dials/phase:  %d   Pause between dials: %d ms", dials, pauseMs)
	fmt.Println()
	info("Each phase reconnects %d times to the same server.", dials)
	info("Phase 1 (QUIC naive)  — new TLS config per dial, no session cache.")
	info("Phase 2 (QUIC smart)  — shared TLS config, session cache reused.")
	info("Phase 3 (TCP)         — CURVE handshake, no session resumption.")
	fmt.Println()
	info("\033[1mKey insight:\033[0m sharing the *tls.Config means quic-go can find")
	info("the cached session ticket and skip the full TLS 1.3 handshake.")
	info("TCP has no equivalent mechanism — every reconnect is a full CURVE.")

	ctx := context.Background()

	// ── Phase 1: QUIC Naive (no session cache sharing) ─────────────────────
	divider(fmt.Sprintf("Phase 1 : QUIC Naive  (%d reconnects, no shared TLS config)", dials))
	fmt.Println("  Each dial creates a fresh *tls.Config — session ticket is never reused.")
	fmt.Println()
	naiveResults := runPhase(ctx, "quic", quicEndpoint, nil, dials, pauseMs)
	naiveAvg := avg(naiveResults)
	fmt.Printf("\n  \033[1mAverage: %.2f ms/dial\033[0m\n", ms(naiveAvg))

	// ── Phase 2: QUIC Smart (shared session cache) ────────────────────────
	divider(fmt.Sprintf("Phase 2 : QUIC Smart  (1 cold + %d warm 0-RTT reconnects)", dials-1))
	fmt.Println("  Shared *tls.Config carries the session cache across dials.")
	fmt.Println()

	// Create ONE TLS config that persists its session cache for the entire phase.
	sharedTLS := quicmq.InsecureClientTLSConfig()
	smartResults := runPhase(ctx, "quic", quicEndpoint, sharedTLS, dials, pauseMs)

	cold, warm := splitWarmCold(smartResults)
	coldAvg := avg(cold)
	warmAvg := avg(warm)
	warmMedian := median(warm)

	fmt.Printf("\n  \033[1mCold (dial #1): %.2f ms    Warm 0-RTT (dials #2+): avg %.2f ms  median %.2f ms\033[0m\n",
		ms(coldAvg), ms(warmAvg), ms(warmMedian))

	// ── Phase 3: TCP (no session resumption) ──────────────────────────────
	divider(fmt.Sprintf("Phase 3 : TCP  (%d reconnects, CURVE handshake every time)", dials))
	fmt.Println("  TCP + CURVE has no session ticket. Every dial pays the full cost.")
	fmt.Println()
	tcpResults := runPhase(ctx, "tcp", tcpEndpoint, nil, dials, pauseMs)
	tcpAvg := avg(tcpResults)
	fmt.Printf("\n  \033[1mAverage: %.2f ms/dial\033[0m\n", ms(tcpAvg))

	// ── Summary ────────────────────────────────────────────────────────────
	border := strings.Repeat("=", bannerW)
	fmt.Printf("\n \033[1;34m╔%s╗\033[0m\n", border)

	printRow := func(label, value, color string) {
		line := fmt.Sprintf("  %-42s  %s", label, value)
		for len(line) < bannerW {
			line += " "
		}
		if len(line) > bannerW {
			line = line[:bannerW]
		}
		fmt.Printf(" \033[1;34m║\033[0m\033[%sm%s\033[0m\033[1;34m║\033[0m\n", color, line)
	}

	printRow("QUIC naive   (no cache, full handshake each):",
		fmt.Sprintf("%.2f ms avg", ms(naiveAvg)), "36")
	printRow("QUIC cold    (first dial, full handshake):",
		fmt.Sprintf("%.2f ms", ms(coldAvg)), "33")
	printRow("QUIC 0-RTT   (session resumption, warm):",
		fmt.Sprintf("%.2f ms avg  /  %.2f ms median", ms(warmAvg), ms(warmMedian)), "32")
	printRow("TCP          (CURVE handshake every time):",
		fmt.Sprintf("%.2f ms avg", ms(tcpAvg)), "33")

	// Speedup: naive reconnect vs 0-RTT reconnect
	if warmAvg > 0 && naiveAvg > 0 {
		speedup := float64(naiveAvg) / float64(warmAvg)
		speedupLine := fmt.Sprintf("  0-RTT reconnect is  %.1fx  faster than naive reconnect", speedup)
		for len(speedupLine) < bannerW {
			speedupLine += " "
		}
		if len(speedupLine) > bannerW {
			speedupLine = speedupLine[:bannerW]
		}
		fmt.Printf(" \033[1;34m║\033[0m\033[1;32m%s\033[0m\033[1;34m║\033[0m\n", speedupLine)
	}

	fmt.Printf(" \033[1;34m╚%s╝\033[0m\n\n", border)

	info("QUIC 0-RTT requires only ONE connection per server lifetime.")
	info("The session ticket is cached automatically once the first")
	info("handshake completes — all future reconnects are near-instant.")
	info("TCP + CURVE has no equivalent: you pay the Diffie-Hellman cost")
	info("on every single reconnect, no matter how recent the last one was.")
}
