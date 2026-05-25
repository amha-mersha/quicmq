// pub — QuicMQ stream-PUB scenario program.
//
// Configuration is entirely through environment variables so Docker Compose
// can parameterise each scenario without rebuilding the image.
//
// Environment variables:
//
//	LISTEN_ADDR   quic://0.0.0.0:9900   Address to listen on
//	TOPIC         data                  Topic prefix to publish
//	MSG_RATE      500                   Target messages per second
//	MSG_SIZE      256                   Payload size in bytes
//	DURATION      30                    Run duration in seconds
//	SCENARIO      custom                Label written into the JSON result
//
// Output: JSON result to stdout + /results/pub-<hostname>.json
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"quicmq"
)

func main() {
	listenAddr := env("LISTEN_ADDR", "quic://0.0.0.0:9900")
	topic := env("TOPIC", "data")
	msgRate, _ := strconv.Atoi(env("MSG_RATE", "500"))
	msgSize, _ := strconv.Atoi(env("MSG_SIZE", "256"))
	durSec, _ := strconv.Atoi(env("DURATION", "30"))
	scenario := env("SCENARIO", "custom")

	dur := time.Duration(durSec) * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pub := quicmq.NewPub(ctx)
	defer pub.Close()

	if err := pub.Listen(listenAddr); err != nil {
		fatalf("pub listen %q: %v", listenAddr, err)
	}
	logf("listening on %s | topic=%s rate=%d/s size=%dB dur=%s",
		pub.Addr(), topic, msgRate, msgSize, dur)

	// Build a fixed-size payload with random bytes so compression doesn't skew throughput.
	padding := make([]byte, max(0, msgSize-64))
	rand.Read(padding)

	start := time.Now()
	deadline := start.Add(dur)

	var sent, errs int64
	seq := int64(0)

	// Use a ticker for pacing.  At very high rates (>10k/s) the ticker
	// resolution may limit accuracy; that's acceptable for scenario testing.
	interval := time.Second / time.Duration(msgRate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for time.Now().Before(deadline) {
		select {
		case <-ticker.C:
			seq++
			// Frame 0: "<topic>|<seq>|<send_ns>" padded to msgSize bytes.
			// The topic prefix makes quicmq's subscription filter match.
			header := fmt.Sprintf("%s|%d|%d", topic, seq, time.Now().UnixNano())
			frame := make([]byte, msgSize)
			n := copy(frame, header)
			copy(frame[n:], padding) // zero-fill + random padding
			if err := pub.Send(quicmq.NewMsg(frame)); err != nil {
				errs++
			} else {
				sent++
			}
		case <-ctx.Done():
			goto done
		}
	}
done:

	elapsed := time.Since(start).Seconds()
	result := map[string]any{
		"scenario":     scenario,
		"role":         "pub",
		"listen_addr":  listenAddr,
		"topic":        topic,
		"config_rate":  msgRate,
		"config_size":  msgSize,
		"duration_s":   elapsed,
		"msgs_sent":    sent,
		"errors":       errs,
		"actual_rate":  float64(sent) / elapsed,
		"throughput_mbs": float64(sent) * float64(msgSize) / elapsed / 1e6,
		"network":      netCfg(),
	}
	writeResult("pub", result)
}

// ── helpers ───────────────────────────────────────────────────────────────────

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func logf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "[pub] "+format+"\n", a...)
}

func fatalf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "[pub] FATAL: "+format+"\n", a...)
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

	hostname, _ := os.Hostname()
	path := fmt.Sprintf("/results/%s-%s.json", role, hostname)
	_ = os.MkdirAll("/results", 0o755)
	_ = os.WriteFile(path, data, 0o644)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
