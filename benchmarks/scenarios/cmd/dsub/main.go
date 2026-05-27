// dsub — QuicMQ QUIC-datagram SUB scenario program (RFC 9221).
//
// Connects to a DatagramPub socket and receives unreliable datagrams for
// DURATION seconds.  Like sub/main.go, it parses the publisher timestamp to
// compute one-way latency and counts sequence gaps.  Because datagrams are
// unreliable, gaps are expected under packet loss — this program quantifies
// the actual delivery ratio, which is the key thesis metric for datagram mode.
//
// Environment variables: identical to sub/main.go with SERVER_ADDR default
// quic://dpub:9902.
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
	serverAddr := env("SERVER_ADDR", "quic://dpub:9902")
	topic := env("TOPIC", "data")
	durSec, _ := strconv.Atoi(env("DURATION", "30"))
	scenario := env("SCENARIO", "custom")

	dur := time.Duration(durSec)*time.Second + 2*time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub := quicmq.NewDatagramSub(ctx,
		quicmq.WithDialerMaxRetries(-1),
		quicmq.WithDialTimeout(30*time.Second),
		quicmq.WithTimeout(500*time.Millisecond),
	)
	defer sub.Close()

	logf("connecting to %s | topic=%s dur=%s", serverAddr, topic, dur)
	if err := sub.Dial(serverAddr); err != nil {
		fatalf("dial %q: %v", serverAddr, err)
	}
	if err := sub.SetOption(quicmq.OptionSubscribe, topic); err != nil {
		fatalf("subscribe: %v", err)
	}
	logf("connected and subscribed to %q (datagram mode)", topic)

	start := time.Now()
	deadline := start.Add(dur)

	var received int64
	var latenciesNs []int64
	var seqGaps int64
	lastSeq := int64(-1)

	for time.Now().Before(deadline) {
		msg, err := sub.Recv()
		if err != nil {
			continue
		}
		recvNs := time.Now().UnixNano()
		received++

		if len(msg.Frames) > 0 {
			header := strings.SplitN(string(msg.Frames[0]), "|", 3)
			if len(header) == 3 {
				seq, errSeq := strconv.ParseInt(header[1], 10, 64)
				sendNs, errTs := strconv.ParseInt(header[2], 10, 64)
				if errSeq == nil && errTs == nil {
					latNs := recvNs - sendNs
					if latNs > 0 && latNs < int64(30*time.Second) {
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
	sort.Slice(latenciesNs, func(i, j int) bool { return latenciesNs[i] < latenciesNs[j] })

	result := map[string]any{
		"scenario":        scenario,
		"role":            "dsub",
		"transport":       "datagram",
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
	writeResult("dsub", result)
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
	fmt.Fprintf(os.Stderr, "[dsub] "+format+"\n", a...)
}

func fatalf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "[dsub] FATAL: "+format+"\n", a...)
	os.Exit(1)
}

func netCfg() map[string]string {
	return map[string]string{
		"delay_ms":  env("NETEM_DELAY_MS", "0"),
		"loss_pct":  env("NETEM_LOSS_PCT", "0"),
		"rate_kbit": env("NETEM_RATE_KBIT", "0"),
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
