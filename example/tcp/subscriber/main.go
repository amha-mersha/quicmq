// tcp/subscriber — connects to a TCP+CURVE publisher and records per-message
// latency statistics to a JSON file for thesis analysis.
//
// The subscriber reads the server public key from -key-file (written by the
// publisher at startup) so multiple subscribers can be launched in parallel
// without manual key distribution.
//
// Expected message format: <topic>|<seq>|<send_ns>|<padding>
//
// Usage:
//
//	go run ./example/tcp/subscriber -key-file server_pk.hex -id sub1
//
// Flags:
//
//	-addr         tcp://127.0.0.1:9200   publisher address
//	-topic        news                    subscription prefix ("" = all)
//	-key-file     server_pk.hex           path containing server public key (hex)
//	-dur          30s                     receive duration
//	-skip-warmup  3s                      discard messages received during this initial
//	                                      period — removes connection-setup latency from
//	                                      stats so post-handshake comparison is fair
//	-output       ""                      JSON result file (stdout if empty)
//	-id           subscriber              node identifier in JSON result
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"quicmq"
)

func main() {
	addr       := flag.String("addr",        "tcp://127.0.0.1:9200", "publisher TCP address")
	topic      := flag.String("topic",      "news",                 "subscription topic prefix")
	keyFile    := flag.String("key-file",   "server_pk.hex",        "file with server public key hex")
	dur        := flag.Duration("dur",      30*time.Second,          "receive duration")
	skipWarmup := flag.Duration("skip-warmup", 3*time.Second,        "discard samples during this initial period")
	output     := flag.String("output",     "",                     "JSON result file (stdout if empty)")
	id         := flag.String("id",         "subscriber",           "node identifier in JSON output")
	flag.Parse()

	pkHex, err := os.ReadFile(*keyFile)
	if err != nil {
		log.Fatalf("[%s] read key file %q: %v", *id, *keyFile, err)
	}
	pkBytes, err := hex.DecodeString(strings.TrimSpace(string(pkHex)))
	if err != nil || len(pkBytes) != 32 {
		log.Fatalf("[%s] invalid public key in %q: %v", *id, *keyFile, err)
	}
	var serverPK [32]byte
	copy(serverPK[:], pkBytes)

	clientKey, err := quicmq.GenerateCurveKey()
	if err != nil {
		log.Fatalf("[%s] generate curve key: %v", *id, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *dur+5*time.Second)
	defer cancel()

	sub := quicmq.NewSub(ctx,
		quicmq.WithCurveClient(clientKey, serverPK),
		quicmq.WithAutomaticReconnect(true),
		quicmq.WithDialTimeout(20*time.Second),
	)
	defer sub.Close()

	if err := sub.Dial(*addr); err != nil {
		log.Fatalf("[%s] dial: %v", *id, err)
	}
	if err := sub.SetOption(quicmq.OptionSubscribe, *topic); err != nil {
		log.Fatalf("[%s] subscribe: %v", *id, err)
	}
	log.Printf("[%s] TCP+CURVE connected to %s  topic=%q  dur=%s", *id, *addr, *topic, *dur)

	type sample struct {
		latMs float64
		seq   int64
	}
	msgCh := make(chan sample, 4096)

	runCtx, runCancel := context.WithTimeout(ctx, *dur)
	defer runCancel()

	go func() {
		for {
			msg, err := sub.Recv()
			if err != nil {
				if runCtx.Err() != nil {
					return
				}
				log.Printf("[%s] recv: %v", *id, err)
				continue
			}
			recvNs := time.Now().UnixNano()
			parts := strings.SplitN(string(msg.Frames[0]), "|", 4)
			if len(parts) < 3 {
				continue
			}
			seq, e1    := strconv.ParseInt(parts[1], 10, 64)
			sentNs, e2 := strconv.ParseInt(parts[2], 10, 64)
			if e1 != nil || e2 != nil {
				continue
			}
			select {
			case msgCh <- sample{latMs: float64(recvNs-sentNs) / 1e6, seq: seq}:
			default:
			}
		}
	}()

	var (
		allLats                  []float64
		totalRcvd, gaps, warmupDiscarded int64
		lastSeq                  int64
	)
	report := time.NewTicker(time.Second)
	defer report.Stop()

	start    := time.Now()
	warmupEnd := start.Add(*skipWarmup)
	winLats  := []float64{}

	if *skipWarmup > 0 {
		log.Printf("[%s] warming up for %s — samples discarded until %s",
			*id, *skipWarmup, warmupEnd.Format(time.TimeOnly))
	}

loop:
	for {
		select {
		case <-runCtx.Done():
			break loop
		case <-report.C:
			printWindow(*id, totalRcvd, gaps, winLats)
			winLats = winLats[:0]
		case s := <-msgCh:
			totalRcvd++
			if lastSeq > 0 && s.seq > lastSeq+1 {
				gaps += s.seq - lastSeq - 1
			}
			lastSeq = s.seq
			// Skip warmup period — these messages are affected by connection
			// setup / handshake overhead and should not be included in latency
			// stats when comparing post-handshake performance.
			if time.Now().Before(warmupEnd) {
				warmupDiscarded++
				continue
			}
			allLats = append(allLats, s.latMs)
			winLats = append(winLats, s.latMs)
		}
	}

	elapsed := time.Since(start).Seconds()

	result := map[string]any{
		"id":               *id,
		"role":             "sub",
		"transport":        "tcp+curve",
		"addr":             *addr,
		"topic":            *topic,
		"skip_warmup_s":    skipWarmup.Seconds(),
		"duration_s":       elapsed,
		"msgs_received":    totalRcvd,
		"warmup_discarded": warmupDiscarded,
		"seq_gaps":         gaps,
		"actual_rate":      float64(totalRcvd) / elapsed,
	}
	if len(allLats) > 0 {
		sort.Float64s(allLats)
		result["latency_p50_ms"] = percentile(allLats, 0.50)
		result["latency_p95_ms"] = percentile(allLats, 0.95)
		result["latency_p99_ms"] = percentile(allLats, 0.99)
		result["latency_max_ms"] = allLats[len(allLats)-1]
	}

	printWindow(*id, totalRcvd, gaps, allLats)
	data, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(data))
	if *output != "" {
		if err := os.WriteFile(*output, data, 0o644); err != nil {
			log.Printf("[%s] write output: %v", *id, err)
		} else {
			log.Printf("[%s] result → %s", *id, *output)
		}
	}
}

func printWindow(id string, total, gaps int64, lats []float64) {
	if len(lats) == 0 {
		fmt.Printf("[%s] rcvd=%d gaps=%d (no samples)\n", id, total, gaps)
		return
	}
	fmt.Printf("[%s] rcvd=%-6d gaps=%-4d p50=%.2fms p95=%.2fms p99=%.2fms max=%.2fms\n",
		id, total, gaps,
		percentile(lats, 0.50), percentile(lats, 0.95),
		percentile(lats, 0.99), lats[len(lats)-1])
}

func percentile(vals []float64, p float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	s := make([]float64, len(vals))
	copy(s, vals)
	sort.Float64s(s)
	idx := int(math.Ceil(p*float64(len(s)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(s) {
		idx = len(s) - 1
	}
	return s[idx]
}
