// fanout/publisher — publishes timestamped messages to a topic at a fixed rate.
//
// Each message payload is:
//
//	<topic>:<unix-nanoseconds>:<sequence>:<payload>
//
// Subscribers decode the nanosecond timestamp to compute end-to-end latency.
// This example demonstrates QUIC's per-subscriber stream independence: each
// subscriber gets its own QUIC stream, so a slow subscriber cannot block a fast
// one (unlike TCP where a single congestion window is shared).
//
// Usage:
//
//	go run ./example/fanout/publisher  [flags]
//
// Flags:
//
//	-addr   quic://0.0.0.0:9100   listen address
//	-topic  bench                  subscription topic
//	-rate   1000                   messages per second
//	-size   256                    extra payload bytes appended to each message
//	-dur    30s                    how long to publish (0 = forever)
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"quicmq"
)

func main() {
	addr  := flag.String("addr",  "quic://0.0.0.0:9100", "listen address")
	topic := flag.String("topic", "bench",               "message topic")
	rate  := flag.Int("rate",    1000,                   "messages per second")
	size  := flag.Int("size",    256,                    "extra payload bytes per message")
	dur   := flag.Duration("dur", 0,                     "publish duration (0 = run forever)")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pub := quicmq.NewPub(ctx)
	defer pub.Close()

	if err := pub.Listen(*addr); err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("fanout/publisher: listening on %s  topic=%s  rate=%d/s  size=%dB",
		pub.Addr(), *topic, *rate, *size)

	padding := strings.Repeat("x", *size)
	interval := time.Second / time.Duration(*rate)
	ticker   := time.NewTicker(interval)
	defer ticker.Stop()

	var deadline <-chan time.Time
	if *dur > 0 {
		deadline = time.After(*dur)
	}

	var seq int64
	for {
		select {
		case <-deadline:
			log.Printf("fanout/publisher: sent %d messages — done", seq)
			return
		case t := <-ticker.C:
			seq++
			payload := fmt.Sprintf("%s:%d:%d:%s", *topic, t.UnixNano(), seq, padding)
			if err := pub.Send(quicmq.NewMsgString(payload)); err != nil {
				log.Printf("send seq=%d: %v", seq, err)
			}
		}
	}
}
