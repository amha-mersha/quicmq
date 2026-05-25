// fanout/subscriber — connects to the fanout publisher and measures end-to-end
// message latency.
//
// Expected message format:  <topic>:<unix-nanoseconds>:<sequence>:<payload>
//
// Extracts the send-timestamp from each message, computes one-way latency, and
// prints a running summary every second.  Run multiple instances with run.sh to
// demonstrate QUIC's per-stream flow-control independence: a slow subscriber
// cannot block others because each has its own QUIC stream.
//
// Usage:
//
//	go run ./example/fanout/subscriber  [flags]
//
// Flags:
//
//	-addr   quic://127.0.0.1:9100   publisher address
//	-topic  bench                    subscription prefix ("" = all)
//	-dur    30s                      how long to receive (0 = run forever)
//	-id     ""                       label printed in output lines
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"quicmq"
)

func main() {
	addr  := flag.String("addr",  "quic://127.0.0.1:9100", "publisher address")
	topic := flag.String("topic", "bench",                 "subscription topic prefix")
	dur   := flag.Duration("dur", 0,                       "receive duration (0 = run forever)")
	id    := flag.String("id",    "",                      "subscriber label for output")
	flag.Parse()

	label := *id
	if label == "" {
		label = fmt.Sprintf("sub-%d", time.Now().UnixNano()%9999+1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	if *dur > 0 {
		ctx, cancel = context.WithTimeout(ctx, *dur)
	}
	defer cancel()

	sub := quicmq.NewSub(ctx,
		quicmq.WithAutomaticReconnect(true),
		quicmq.WithDialTimeout(30*time.Second),
	)
	defer sub.Close()

	if err := sub.Dial(*addr); err != nil {
		log.Fatalf("[%s] dial: %v", label, err)
	}
	if err := sub.SetOption(quicmq.OptionSubscribe, *topic); err != nil {
		log.Fatalf("[%s] subscribe: %v", label, err)
	}
	log.Printf("[%s] connected to %s  topic=%q", label, *addr, *topic)

	type sample struct {
		latMs float64
		seq   int64
	}
	msgCh := make(chan sample, 1024)

	// Recv loop in a separate goroutine so the main goroutine can drive the
	// ticker without blocking on a slow Recv.
	go func() {
		for {
			msg, err := sub.Recv()
			if err != nil {
				if ctx.Err() != nil {
					return // context cancelled/deadline exceeded — normal exit
				}
				log.Printf("[%s] recv error: %v", label, err)
				continue
			}
			recvAt := time.Now().UnixNano()
			parts := strings.SplitN(string(msg.Frames[0]), ":", 4)
			if len(parts) < 3 {
				continue
			}
			sentNs, err2 := strconv.ParseInt(parts[1], 10, 64)
			seq, err3 := strconv.ParseInt(parts[2], 10, 64)
			if err2 != nil || err3 != nil {
				continue
			}
			select {
			case msgCh <- sample{latMs: float64(recvAt-sentNs) / 1e6, seq: seq}:
			default: // drop if consumer is slow — avoids unbounded buffering
			}
		}
	}()

	report := time.NewTicker(time.Second)
	defer report.Stop()

	var (
		count     int64
		gaps      int64
		lastSeq   int64
		winLats   []float64 // latencies in current 1-second window
		totalLats []float64 // all latencies — for final summary
	)

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("\n[%s] === FINAL SUMMARY ===\n", label)
			printStats(label, count, gaps, totalLats)
			return

		case <-report.C:
			printStats(label, count, gaps, winLats)
			winLats = winLats[:0]

		case s := <-msgCh:
			count++
			winLats = append(winLats, s.latMs)
			totalLats = append(totalLats, s.latMs)
			if lastSeq > 0 && s.seq != lastSeq+1 {
				gaps += s.seq - lastSeq - 1
			}
			lastSeq = s.seq
		}
	}
}

func printStats(label string, count, gaps int64, latencies []float64) {
	if len(latencies) == 0 {
		fmt.Printf("[%s] rcvd=%d  gaps=%d  (no samples this window)\n", label, count, gaps)
		return
	}
	sorted := make([]float64, len(latencies))
	copy(sorted, latencies)
	sort.Float64s(sorted)
	n := len(sorted)
	p50 := sorted[n/2]
	p95 := sorted[clamp(int(math.Ceil(float64(n)*0.95))-1, 0, n-1)]
	p99 := sorted[clamp(int(math.Ceil(float64(n)*0.99))-1, 0, n-1)]
	min := sorted[0]
	max := sorted[n-1]
	fmt.Printf("[%s] rcvd=%d  gaps=%d  min=%.2fms  p50=%.2fms  p95=%.2fms  p99=%.2fms  max=%.2fms\n",
		label, count, gaps, min, p50, p95, p99, max)
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
