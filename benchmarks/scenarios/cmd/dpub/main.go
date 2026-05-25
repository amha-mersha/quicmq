// dpub — QuicMQ QUIC-datagram PUB scenario program (RFC 9221).
//
// Identical to pub/main.go but uses DatagramPubSocket so messages are sent as
// unreliable QUIC datagrams rather than reliable streams.  Comparing this
// program's scenario results against pub lets the thesis quantify the
// reliability / latency trade-off between the two delivery modes.
//
// Environment variables: identical to pub/main.go with LISTEN_ADDR default
// quic://0.0.0.0:9902.
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
	listenAddr := env("LISTEN_ADDR", "quic://0.0.0.0:9902")
	topic := env("TOPIC", "data")
	msgRate, _ := strconv.Atoi(env("MSG_RATE", "500"))
	msgSize, _ := strconv.Atoi(env("MSG_SIZE", "256"))
	durSec, _ := strconv.Atoi(env("DURATION", "30"))
	scenario := env("SCENARIO", "custom")

	// QUIC datagrams are bounded by the path MTU (~1200 bytes for QUIC).
	// Warn but clamp instead of failing so test configs aren't fragile.
	const maxDatagramPayload = 1200
	if msgSize > maxDatagramPayload {
		logf("WARNING: MSG_SIZE=%d exceeds datagram MTU; clamping to %d", msgSize, maxDatagramPayload)
		msgSize = maxDatagramPayload
	}

	dur := time.Duration(durSec) * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pub := quicmq.NewDatagramPub(ctx)
	defer pub.Close()

	if err := pub.Listen(listenAddr); err != nil {
		fatalf("dpub listen %q: %v", listenAddr, err)
	}
	logf("listening on %s | topic=%s rate=%d/s size=%dB dur=%s",
		pub.Addr(), topic, msgRate, msgSize, dur)

	padding := make([]byte, max(0, msgSize-64))
	rand.Read(padding)

	start := time.Now()
	deadline := start.Add(dur)

	var sent, errs int64
	seq := int64(0)

	interval := time.Second / time.Duration(msgRate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for time.Now().Before(deadline) {
		select {
		case <-ticker.C:
			seq++
			header := fmt.Sprintf("%s|%d|%d", topic, seq, time.Now().UnixNano())
			frame := make([]byte, msgSize)
			n := copy(frame, header)
			copy(frame[n:], padding)
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
		"scenario":       scenario,
		"role":           "dpub",
		"transport":      "datagram",
		"listen_addr":    listenAddr,
		"topic":          topic,
		"config_rate":    msgRate,
		"config_size":    msgSize,
		"duration_s":     elapsed,
		"msgs_sent":      sent,
		"errors":         errs,
		"actual_rate":    float64(sent) / elapsed,
		"throughput_mbs": float64(sent) * float64(msgSize) / elapsed / 1e6,
		"network":        netCfg(),
	}
	writeResult("dpub", result)
}

// ── helpers ───────────────────────────────────────────────────────────────────

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func logf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "[dpub] "+format+"\n", a...)
}

func fatalf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "[dpub] FATAL: "+format+"\n", a...)
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
