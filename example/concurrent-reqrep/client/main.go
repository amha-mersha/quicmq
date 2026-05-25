// concurrent-reqrep/client — sends concurrent REQ/REP requests and records
// per-request RTT statistics to a JSON file.
//
// Each -workers goroutine owns its own quicmq.Req socket (one QUIC connection
// per socket, one stream per request).  Workers run in parallel, demonstrating
// that QUIC streams for different requests are independent.
//
// Usage:
//
//	go run ./example/concurrent-reqrep/client -workers 10 -count 200 -id client1
//
// Or use the run script:
//
//	./example/concurrent-reqrep/run.sh --clients 4 --workers 10 --count 200
//
// Flags:
//
//	-addr     quic://127.0.0.1:9400   server address
//	-workers  5                        concurrent REQ goroutines in this process
//	-count    100                      requests per worker (-1 = run until -dur)
//	-dur      30s                      max run duration
//	-size     64                       request payload bytes
//	-output   ""                       JSON result file (stdout if empty)
//	-id       client                   node identifier in JSON output
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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"quicmq"
)

func main() {
	addr       := flag.String("addr",        "quic://127.0.0.1:9400", "server address")
	workers    := flag.Int("workers",        5,                        "concurrent REQ goroutines")
	count      := flag.Int("count",          100,                      "requests per worker (-1 = unlimited)")
	dur        := flag.Duration("dur",       30*time.Second,            "max run duration")
	skipWarmup := flag.Duration("skip-warmup", 2*time.Second,           "discard RTT samples during initial period")
	size       := flag.Int("size",           64,                        "request payload bytes")
	output     := flag.String("output",      "",                       "JSON result file (stdout if empty)")
	id         := flag.String("id",          "client",                 "node identifier in JSON output")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()

	var (
		mu       sync.Mutex
		allRTTs  []float64
		totalErr atomic.Int64
		sent     atomic.Int64
	)

	payload := strings.Repeat("x", *size)
	start     := time.Now()
	warmupEnd := start.Add(*skipWarmup)
	if *skipWarmup > 0 {
		log.Printf("[%s] warming up for %s — RTT samples discarded until %s",
			*id, *skipWarmup, warmupEnd.Format(time.TimeOnly))
	}

	var warmupDiscarded atomic.Int64
	var wg sync.WaitGroup

	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()

			req := quicmq.NewReq(ctx,
				quicmq.WithDialTimeout(20*time.Second),
				quicmq.WithDialerRetry(200*time.Millisecond),
				quicmq.WithDialerMaxRetries(-1),
			)
			defer req.Close()

			if err := req.Dial(*addr); err != nil {
				log.Printf("[%s/w%d] dial: %v", *id, wid, err)
				totalErr.Add(1)
				return
			}

			for i := 0; *count < 0 || i < *count; i++ {
				if ctx.Err() != nil {
					return
				}
				msg := fmt.Sprintf("w%d-req%d-%s", wid, i, payload)
				t0  := time.Now()
				if err := req.Send(quicmq.NewMsgString(msg)); err != nil {
					totalErr.Add(1)
					continue
				}
				if _, err := req.Recv(); err != nil {
					totalErr.Add(1)
					continue
				}
				sent.Add(1)
				if time.Now().Before(warmupEnd) {
					warmupDiscarded.Add(1)
					continue
				}
				rttMs := float64(time.Since(t0).Nanoseconds()) / 1e6
				mu.Lock()
				allRTTs = append(allRTTs, rttMs)
				mu.Unlock()
			}
		}(w)
	}

	// Per-second progress report.
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var last int64
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				n := sent.Load()
				mu.Lock()
				subset := make([]float64, len(allRTTs))
				copy(subset, allRTTs)
				mu.Unlock()
				p50, p99 := 0.0, 0.0
				if len(subset) > 0 {
					sort.Float64s(subset)
					p50 = percentile(subset, 0.50)
					p99 = percentile(subset, 0.99)
				}
				log.Printf("[%s] %d req/s  total=%d  p50=%.2fms  p99=%.2fms  err=%d",
					*id, n-last, n, p50, p99, totalErr.Load())
				last = n
			}
		}
	}()

	wg.Wait()
	cancel()

	elapsed := time.Since(start).Seconds()
	total   := sent.Load()
	errs    := totalErr.Load()

	mu.Lock()
	finalRTTs := make([]float64, len(allRTTs))
	copy(finalRTTs, allRTTs)
	mu.Unlock()

	result := map[string]any{
		"id":               *id,
		"role":             "req",
		"transport":        "quic",
		"addr":             *addr,
		"workers":          *workers,
		"config_count":     *count,
		"config_size":      *size,
		"skip_warmup_s":    skipWarmup.Seconds(),
		"duration_s":       elapsed,
		"reqs_sent":        total,
		"warmup_discarded": warmupDiscarded.Load(),
		"errors":           errs,
		"actual_rate":      float64(total) / elapsed,
	}
	if len(finalRTTs) > 0 {
		sort.Float64s(finalRTTs)
		result["rtt_p50_ms"] = percentile(finalRTTs, 0.50)
		result["rtt_p95_ms"] = percentile(finalRTTs, 0.95)
		result["rtt_p99_ms"] = percentile(finalRTTs, 0.99)
		result["rtt_max_ms"] = finalRTTs[len(finalRTTs)-1]
		var sum float64
		for _, v := range finalRTTs { sum += v }
		result["rtt_avg_ms"] = sum / float64(len(finalRTTs))
	}

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
