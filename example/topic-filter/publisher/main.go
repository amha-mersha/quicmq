// topic-filter/publisher — publishes to multiple configurable topics and records
// per-topic send statistics to a JSON file.
//
// Messages embed a send timestamp so subscribers can compute one-way latency.
// Message format: <topic>|<seq>|<send_ns>|<padding>
//
// Usage:
//
//	terminal 1: go run ./example/topic-filter/publisher -topics sports,finance,weather -rate 300
//	terminal 2: go run ./example/topic-filter/subscriber -topics sports
//	terminal 3: go run ./example/topic-filter/subscriber -topics finance,weather -id sub2
//
// Or use the run script:
//
//	./example/topic-filter/run.sh --topics sports,finance,weather --subs 3
//
// Flags:
//
//	-addr    quic://0.0.0.0:9500   listen address
//	-topics  sports,finance,weather comma-separated list of topics to publish
//	-rate    300                    total messages per second (split across topics)
//	-size    128                    payload bytes per message
//	-dur     30s                    publish duration
//	-output  ""                     JSON result file (stdout if empty)
//	-id      publisher              node identifier in JSON output
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"quicmq"
)

func main() {
	addr   := flag.String("addr",   "quic://0.0.0.0:9500",      "listen address")
	topics := flag.String("topics", "sports,finance,weather",    "comma-separated topic list")
	rate   := flag.Int("rate",      300,                         "total messages per second")
	size   := flag.Int("size",      128,                         "payload bytes per message")
	dur    := flag.Duration("dur",  30*time.Second,              "publish duration")
	output := flag.String("output", "",                          "JSON result file (stdout if empty)")
	id     := flag.String("id",     "publisher",                 "node identifier in JSON output")
	flag.Parse()

	topicList := strings.Split(*topics, ",")
	for i, t := range topicList {
		topicList[i] = strings.TrimSpace(t)
	}
	if len(topicList) == 0 {
		log.Fatal("at least one topic required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *dur+5*time.Second)
	defer cancel()

	pub := quicmq.NewPub(ctx)
	defer pub.Close()

	if err := pub.Listen(*addr); err != nil {
		log.Fatalf("[%s] listen: %v", *id, err)
	}
	log.Printf("[%s] publisher on %s  topics=%v  rate=%d/s  size=%dB  dur=%s",
		*id, pub.Addr(), topicList, *rate, *size, *dur)

	padding := make([]byte, max(0, *size-64))
	rand.Read(padding)

	interval := time.Second / time.Duration(*rate)
	ticker   := time.NewTicker(interval)
	defer ticker.Stop()

	runCtx, runCancel := context.WithTimeout(ctx, *dur)
	defer runCancel()

	start := time.Now()
	perTopicSent := make(map[string]int64, len(topicList))
	var totalSent, errors int64
	var seq int64

	for {
		select {
		case <-runCtx.Done():
			goto done
		case t := <-ticker.C:
			seq++
			topic := topicList[seq%int64(len(topicList))]
			header := fmt.Sprintf("%s|%d|%d", topic, seq, t.UnixNano())
			frame := make([]byte, *size)
			n := copy(frame, header)
			copy(frame[n:], padding)
			if err := pub.Send(quicmq.NewMsg(frame)); err != nil {
				errors++
			} else {
				perTopicSent[topic]++
				totalSent++
			}
		}
	}
done:
	elapsed := time.Since(start).Seconds()

	topicStats := make(map[string]any, len(topicList))
	for t, c := range perTopicSent {
		topicStats[t] = map[string]any{
			"sent": c,
			"rate": float64(c) / elapsed,
		}
	}

	result := map[string]any{
		"id":             *id,
		"role":           "pub",
		"transport":      "quic",
		"addr":           pub.Addr().String(),
		"topics":         topicList,
		"config_rate":    *rate,
		"config_size":    *size,
		"duration_s":     elapsed,
		"msgs_sent":      totalSent,
		"errors":         errors,
		"actual_rate":    float64(totalSent) / elapsed,
		"throughput_mbs": float64(totalSent) * float64(*size) / elapsed / 1e6,
		"per_topic":      topicStats,
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

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
