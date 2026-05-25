// rep — QuicMQ REP scenario server.
//
// Listens for REQ connections and echoes every request back as a reply.
// Runs for DURATION seconds, then exits and reports metrics.
//
// Environment variables:
//
//	LISTEN_ADDR   quic://0.0.0.0:9901   Address to listen on
//	DURATION      30                     Run duration in seconds
//	SCENARIO      custom                 Label written into the JSON result
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"quicmq"
)

func main() {
	listenAddr := env("LISTEN_ADDR", "quic://0.0.0.0:9901")
	durSec, _ := strconv.Atoi(env("DURATION", "30"))
	scenario := env("SCENARIO", "custom")

	dur := time.Duration(durSec) * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), dur+5*time.Second)
	defer cancel()

	rep := quicmq.NewRep(ctx,
		quicmq.WithTimeout(500*time.Millisecond),
	)
	defer rep.Close()

	if err := rep.Listen(listenAddr); err != nil {
		fatalf("rep listen %q: %v", listenAddr, err)
	}
	logf("listening on %s | dur=%s", rep.Addr(), dur)

	start := time.Now()
	deadline := start.Add(dur)

	var handled, errs int64

	for time.Now().Before(deadline) {
		msg, err := rep.Recv()
		if err != nil {
			continue // timeout or transient
		}
		if err := rep.Send(msg); err != nil {
			errs++
		} else {
			handled++
		}
	}

	elapsed := time.Since(start).Seconds()
	result := map[string]any{
		"scenario":       scenario,
		"role":           "rep",
		"listen_addr":    listenAddr,
		"duration_s":     elapsed,
		"reqs_handled":   handled,
		"errors":         errs,
		"actual_rate":    float64(handled) / elapsed,
		"network":        netCfg(),
	}
	writeResult("rep", result)
}

// ── helpers ───────────────────────────────────────────────────────────────────

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func logf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "[rep] "+format+"\n", a...)
}

func fatalf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "[rep] FATAL: "+format+"\n", a...)
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
