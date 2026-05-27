// req — QuicMQ REQ scenario client.
//
// Spawns CONCURRENCY goroutines, each with its own REQ socket, and hammers
// the REP server for DURATION seconds.  Because REQ enforces strict
// send→recv alternation, concurrency requires one socket per goroutine.
//
// Environment variables:
//
//	SERVER_ADDR   quic://rep:9901   REP server address
//	CONCURRENCY   5                 Number of parallel REQ sockets
//	MSG_SIZE      256               Request payload size in bytes
//	DURATION      30                Run duration in seconds
//	SCENARIO      custom            Label written into the JSON result
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"quicmq"
)

func main() {
	serverAddr := env("SERVER_ADDR", "quic://rep:9901")
	concurrency, _ := strconv.Atoi(env("CONCURRENCY", "5"))
	msgSize, _ := strconv.Atoi(env("MSG_SIZE", "256"))
	durSec, _ := strconv.Atoi(env("DURATION", "30"))
	scenario := env("SCENARIO", "custom")

	dur := time.Duration(durSec) * time.Second

	logf("connecting to %s | concurrency=%d size=%dB dur=%s",
		serverAddr, concurrency, msgSize, dur)

	deadline := time.Now().Add(dur)

	var (
		mu          sync.Mutex
		latenciesNs []int64
		totalReqs   int64
		totalErrors int64
	)

	// Padding is shared / reused (random bytes, so no compressibility bias).
	padding := make([]byte, msgSize)
	rand.Read(padding)

	start := time.Now()
	var wg sync.WaitGroup

	for w := range concurrency {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			ctx, cancel := context.WithDeadline(context.Background(), deadline.Add(5*time.Second))
			defer cancel()

			req := quicmq.NewReq(ctx,
				quicmq.WithDialerMaxRetries(-1),
				quicmq.WithDialTimeout(30*time.Second),
				quicmq.WithAutomaticReconnect(false), // REQ FSM state would be lost on reconnect
				quicmq.WithTimeout(2*time.Second),
			)
			defer req.Close()

			if err := req.Dial(serverAddr); err != nil {
				logf("worker %d dial error: %v", id, err)
				return
			}

			var reqs, errors int64
			var lats []int64

			for time.Now().Before(deadline) {
				sendNs := time.Now().UnixNano()
				payload := make([]byte, msgSize)
				copy(payload, padding)

				if err := req.Send(quicmq.NewMsg(payload)); err != nil {
					errors++
					continue
				}
				if _, err := req.Recv(); err != nil {
					errors++
					continue
				}
				rttNs := time.Now().UnixNano() - sendNs
				reqs++
				lats = append(lats, rttNs)
			}

			mu.Lock()
			latenciesNs = append(latenciesNs, lats...)
			totalReqs += reqs
			totalErrors += errors
			mu.Unlock()
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start).Seconds()

	sort.Slice(latenciesNs, func(i, j int) bool { return latenciesNs[i] < latenciesNs[j] })

	result := map[string]any{
		"scenario":       scenario,
		"role":           "req",
		"server_addr":    serverAddr,
		"concurrency":    concurrency,
		"config_size":    msgSize,
		"duration_s":     elapsed,
		"reqs_sent":      totalReqs,
		"errors":         totalErrors,
		"actual_rate":    float64(totalReqs) / elapsed,
		"rtt_p50_ms":     ms(pct(latenciesNs, 50)),
		"rtt_p95_ms":     ms(pct(latenciesNs, 95)),
		"rtt_p99_ms":     ms(pct(latenciesNs, 99)),
		"rtt_max_ms":     ms(pct(latenciesNs, 100)),
		"rtt_samples":    len(latenciesNs),
		"network":        netCfg(),
	}
	writeResult("req", result)
}

func pct(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)) * p / 100.0)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func ms(ns int64) float64 { return float64(ns) / 1e6 }

// ── helpers ───────────────────────────────────────────────────────────────────

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func logf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "[req] "+format+"\n", a...)
}

func netCfg() map[string]string {
	return map[string]string{
		"delay_ms":    env("NETEM_DELAY_MS", "0"),
		"jitter_ms":   env("NETEM_JITTER_MS", "0"),
		"loss_pct":    env("NETEM_LOSS_PCT", "0"),
		"rate_kbit":   env("NETEM_RATE_KBIT", "0"),
		"corrupt_pct": env("NETEM_CORRUPT_PCT", "0"),
		"reorder_pct": env("NETEM_REORDER_PCT", "0"),
	}
}

func writeResult(role string, result map[string]any) {
	data, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(data))

	dir := os.Getenv("RESULTS_DIR")
	if dir == "" {
		dir = "/results"
	}
	hostname, _ := os.Hostname()
	path := fmt.Sprintf("%s/%s-%s.json", dir, role, hostname)
	if err := os.MkdirAll(dir, 0o755); err == nil {
		_ = os.WriteFile(path, data, 0o644)
	}
}
