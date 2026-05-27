// sub — QuicMQ stream-SUB scenario program.
//
// Connects to a PUB socket, subscribes to TOPIC, and receives messages for
// DURATION seconds.  Parses the publisher's header (see pub/main.go) to
// compute per-message one-way latency and detect sequence gaps (packet loss).
//
// Environment variables:
//
//	SERVER_ADDR   quic://pub:9900   Publisher address to connect to
//	TOPIC         data              Subscription topic prefix
//	DURATION      30                Run duration in seconds (extra 2s grace)
//	SCENARIO      custom            Label written into the JSON result
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"quicmq"
)

func main() {
	serverAddr := env("SERVER_ADDR", "quic://pub:9900")
	topic := env("TOPIC", "data")
	durSec, _ := strconv.Atoi(env("DURATION", "30"))
	scenario := env("SCENARIO", "custom")

	dur := time.Duration(durSec)*time.Second + 2*time.Second // 2s grace for in-flight msgs

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub := quicmq.NewSub(ctx,
		quicmq.WithDialerMaxRetries(-1),
		quicmq.WithDialTimeout(30*time.Second),
		quicmq.WithTimeout(500*time.Millisecond), // short recv timeout for deadline loop
	)
	defer sub.Close()

	logf("connecting to %s | topic=%s dur=%s", serverAddr, topic, dur)
	if err := sub.Dial(serverAddr); err != nil {
		fatalf("dial %q: %v", serverAddr, err)
	}
	if err := sub.SetOption(quicmq.OptionSubscribe, topic); err != nil {
		fatalf("subscribe: %v", err)
	}
	logf("connected and subscribed to %q", topic)

	start := time.Now()
	deadline := start.Add(dur)

	var received int64
	var latenciesNs []int64
	var seqGaps int64
	lastSeq := int64(-1)

	for time.Now().Before(deadline) {
		msg, err := sub.Recv()
		if err != nil {
			continue // timeout or transient — keep looping until deadline
		}
		recvNs := time.Now().UnixNano()
		received++

		// Parse header: "<topic>|<seq>|<send_ns>"
		if len(msg.Frames) > 0 {
			header := strings.SplitN(string(msg.Frames[0]), "|", 3)
			if len(header) == 3 {
				seq, errSeq := strconv.ParseInt(header[1], 10, 64)
				sendNs, errTs := strconv.ParseInt(header[2], 10, 64)
				if errSeq == nil && errTs == nil {
					latNs := recvNs - sendNs
					if latNs > 0 && latNs < int64(30*time.Second) { // sanity guard
						latenciesNs = append(latenciesNs, latNs)
					}
					if lastSeq >= 0 && seq > lastSeq+1 {
						seqGaps += seq - (lastSeq + 1)
					}
					lastSeq = seq
				}
			}
		}
	}

	elapsed := time.Since(start).Seconds()

	// Latency percentiles (sort in-place).
	sort.Slice(latenciesNs, func(i, j int) bool { return latenciesNs[i] < latenciesNs[j] })

	result := map[string]any{
		"scenario":        scenario,
		"role":            "sub",
		"server_addr":     serverAddr,
		"topic":           topic,
		"duration_s":      elapsed,
		"msgs_received":   received,
		"seq_gaps":        seqGaps,
		"actual_rate":     float64(received) / elapsed,
		"latency_p50_ms":  ms(pct(latenciesNs, 50)),
		"latency_p95_ms":  ms(pct(latenciesNs, 95)),
		"latency_p99_ms":  ms(pct(latenciesNs, 99)),
		"latency_max_ms":  ms(pct(latenciesNs, 100)),
		"latency_samples": len(latenciesNs),
		"network":         netCfg(),
	}
	writeResult("sub", result)
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
	fmt.Fprintf(os.Stderr, "[sub] "+format+"\n", a...)
}

func fatalf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "[sub] FATAL: "+format+"\n", a...)
	os.Exit(1)
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
