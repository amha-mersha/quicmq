// datagram/publisher — publishes via QUIC RFC 9221 unreliable datagrams and
// records send statistics for thesis comparison against stream-based PUB/SUB.
//
// Datagrams skip retransmission entirely; the subscriber sees raw packet loss
// as sequence gaps.  Run both this publisher and datagram/subscriber, then
// compare with example/pubsub (stream-based) under the same loss conditions.
//
// Usage:
//
//	terminal 1: go run ./example/datagram/publisher -rate 2000
//	terminal 2: go run ./example/datagram/subscriber -id sub1
//	terminal 3: go run ./example/datagram/subscriber -id sub2
//
// Or use the run script:
//
//	./example/datagram/run.sh --subs 5 --rate 2000 --loss 5
//
// Flags:
//
//	-addr    quic://0.0.0.0:9300   listen address
//	-topic   sensor                 topic prefix
//	-rate    1000                   messages per second
//	-size    256                    payload bytes (must fit in QUIC datagram MTU, ≤1200)
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
	"time"

	"quicmq"
)

func main() {
	addr   := flag.String("addr",   "quic://0.0.0.0:9300", "listen address")
	topic  := flag.String("topic",  "sensor",              "topic prefix")
	rate   := flag.Int("rate",      1000,                  "messages per second")
	size   := flag.Int("size",      256,                   "payload bytes (max ~1200 for datagrams)")
	dur    := flag.Duration("dur",  30*time.Second,         "publish duration")
	output := flag.String("output", "",                    "JSON result file (stdout if empty)")
	id     := flag.String("id",     "publisher",           "node identifier in JSON output")
	flag.Parse()

	if *size > 1200 {
		log.Printf("[%s] WARNING: size %dB may exceed QUIC datagram MTU (~1200B)", *id, *size)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *dur+5*time.Second)
	defer cancel()

	pub := quicmq.NewDatagramPub(ctx)
	defer pub.Close()

	if err := pub.Listen(*addr); err != nil {
		log.Fatalf("[%s] listen: %v", *id, err)
	}
	log.Printf("[%s] datagram publisher on %s  topic=%q  rate=%d/s  size=%dB  dur=%s",
		*id, pub.Addr(), *topic, *rate, *size, *dur)
	log.Printf("[%s] unreliable delivery (RFC 9221) — no retransmit on loss", *id)

	padding := make([]byte, max(0, *size-64))
	rand.Read(padding)

	interval := time.Second / time.Duration(*rate)
	ticker   := time.NewTicker(interval)
	defer ticker.Stop()

	runCtx, runCancel := context.WithTimeout(ctx, *dur)
	defer runCancel()

	start := time.Now()
	var sent, errors int64
	var seq int64

	for {
		select {
		case <-runCtx.Done():
			goto done
		case t := <-ticker.C:
			seq++
			header := fmt.Sprintf("%s|%d|%d", *topic, seq, t.UnixNano())
			frame := make([]byte, *size)
			n := copy(frame, header)
			copy(frame[n:], padding)
			if err := pub.Send(quicmq.NewMsg(frame)); err != nil {
				errors++
			} else {
				sent++
			}
		}
	}
done:
	elapsed := time.Since(start).Seconds()

	result := map[string]any{
		"id":             *id,
		"role":           "dpub",
		"transport":      "quic-datagram",
		"addr":           pub.Addr().String(),
		"topic":          *topic,
		"config_rate":    *rate,
		"config_size":    *size,
		"duration_s":     elapsed,
		"msgs_sent":      sent,
		"errors":         errors,
		"actual_rate":    float64(sent) / elapsed,
		"throughput_mbs": float64(sent) * float64(*size) / elapsed / 1e6,
	}
	writeResult(*id, *output, result)
}

func writeResult(id, path string, result map[string]any) {
	data, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(data))
	if path != "" {
		if err := os.WriteFile(path, data, 0o644); err != nil {
			log.Printf("[%s] write output: %v", id, err)
		} else {
			log.Printf("[%s] result → %s", id, path)
		}
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
