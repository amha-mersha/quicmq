// topic-filter/subscriber — subscribes to one or more topic prefixes and
// records per-topic latency statistics to a JSON file.
//
// Multiple subscribers can run simultaneously with different topic sets to
// demonstrate that each receives only its matching messages, while the publisher
// sends all topics at the same rate.
//
// Expected message format: <topic>|<seq>|<send_ns>|<padding>
//
// Usage:
//
//	go run ./example/topic-filter/subscriber -topics sports -id sub1 -output sub1.json
//	go run ./example/topic-filter/subscriber -topics finance,weather -id sub2 -output sub2.json
//
// Flags:
//
//	-addr    quic://127.0.0.1:9500   publisher address
//	-topics  sports                   comma-separated subscription prefixes ("" = all)
//	-dur     30s                      receive duration
//	-output  ""                       JSON result file (stdout if empty)
//	-id      subscriber               node identifier in JSON output
package main

import (
	"context"
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
	addr       := flag.String("addr",        "quic://127.0.0.1:9500", "publisher address")
	topics     := flag.String("topics",     "sports",                "comma-separated subscription prefixes")
	dur        := flag.Duration("dur",      30*time.Second,           "receive duration")
	skipWarmup := flag.Duration("skip-warmup", 3*time.Second,         "discard early samples (post-handshake baseline)")
	output     := flag.String("output",     "",                      "JSON result file (stdout if empty)")
	id         := flag.String("id",         "subscriber",            "node identifier in JSON output")
	flag.Parse()

	prefixes := strings.Split(*topics, ",")
	for i, p := range prefixes {
		prefixes[i] = strings.TrimSpace(p)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *dur+5*time.Second)
	defer cancel()

	sub := quicmq.NewSub(ctx,
		quicmq.WithAutomaticReconnect(true),
		quicmq.WithDialTimeout(20*time.Second),
	)
	defer sub.Close()

	if err := sub.Dial(*addr); err != nil {
		log.Fatalf("[%s] dial: %v", *id, err)
	}
	for _, p := range prefixes {
		if err := sub.SetOption(quicmq.OptionSubscribe, p); err != nil {
			log.Fatalf("[%s] subscribe %q: %v", *id, p, err)
		}
	}
	log.Printf("[%s] connected to %s  subscriptions=%v  dur=%s", *id, *addr, prefixes, *dur)

	type sample struct {
		topic string
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
			case msgCh <- sample{
				topic: parts[0],
				latMs: float64(recvNs-sentNs) / 1e6,
				seq:   seq,
			}:
			default:
			}
		}
	}()

	type topicStats struct {
		lats     []float64
		count    int64
		lastSeq  map[string]int64
		gaps     int64
	}
	stats := map[string]*topicStats{}
	for _, p := range prefixes {
		stats[p] = &topicStats{lastSeq: map[string]int64{}}
	}
	var totalRcvd, warmupDiscarded int64

	report   := time.NewTicker(time.Second)
	defer report.Stop()
	start     := time.Now()
	warmupEnd := start.Add(*skipWarmup)
	winLats   := []float64{}

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
			printWindow(*id, totalRcvd, winLats)
			winLats = winLats[:0]
		case s := <-msgCh:
			totalRcvd++
			if time.Now().Before(warmupEnd) {
				warmupDiscarded++
				continue
			}
			winLats = append(winLats, s.latMs)

			// Find matching prefix bucket.
			for _, p := range prefixes {
				if p == "" || strings.HasPrefix(s.topic, p) {
					st := stats[p]
					st.count++
					st.lats = append(st.lats, s.latMs)
					if last, ok := st.lastSeq[s.topic]; ok && s.seq > last+1 {
						st.gaps += s.seq - last - 1
					}
					st.lastSeq[s.topic] = s.seq
					break
				}
			}
		}
	}

	elapsed := time.Since(start).Seconds()

	perTopicResults := map[string]any{}
	for p, st := range stats {
		r := map[string]any{
			"msgs_received": st.count,
			"seq_gaps":      st.gaps,
			"actual_rate":   float64(st.count) / elapsed,
		}
		if len(st.lats) > 0 {
			sort.Float64s(st.lats)
			r["latency_p50_ms"] = percentile(st.lats, 0.50)
			r["latency_p95_ms"] = percentile(st.lats, 0.95)
			r["latency_p99_ms"] = percentile(st.lats, 0.99)
			r["latency_max_ms"] = st.lats[len(st.lats)-1]
		}
		key := p
		if key == "" {
			key = "(all)"
		}
		perTopicResults[key] = r
	}

	result := map[string]any{
		"id":               *id,
		"role":             "sub",
		"transport":        "quic",
		"addr":             *addr,
		"subscriptions":    prefixes,
		"skip_warmup_s":    skipWarmup.Seconds(),
		"duration_s":       elapsed,
		"msgs_received":    totalRcvd,
		"warmup_discarded": warmupDiscarded,
		"actual_rate":      float64(totalRcvd) / elapsed,
		"per_topic":        perTopicResults,
	}

	printWindow(*id+" FINAL", totalRcvd, winLats)
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

func printWindow(id string, total int64, lats []float64) {
	if len(lats) == 0 {
		fmt.Printf("[%s] rcvd=%d (no samples)\n", id, total)
		return
	}
	fmt.Printf("[%s] rcvd=%-6d p50=%.2fms p99=%.2fms max=%.2fms\n",
		id, total,
		percentile(lats, 0.50), percentile(lats, 0.99), lats[len(lats)-1])
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
