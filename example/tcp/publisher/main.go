// tcp/publisher — publishes messages over TCP+ZMTP with CURVE security and
// records send statistics to a JSON file for thesis comparison against QUIC.
//
// The publisher generates a Curve25519 keypair at startup and writes the
// server public key to -key-file (default: server_pk.hex) so that multiple
// subscriber nodes can read it without manual copy-paste.
//
// Usage (three terminals):
//
//	terminal 1: go run ./example/tcp/publisher -addr tcp://0.0.0.0:9200 -rate 1000
//	terminal 2: go run ./example/tcp/subscriber -key-file server_pk.hex
//	terminal 3: go run ./example/tcp/subscriber -key-file server_pk.hex -id sub2
//
// Or use the bundled run.sh for fully automated multi-node runs:
//
//	./example/tcp/run.sh --subs 5 --rate 2000 --dur 30s
//
// Flags:
//
//	-addr      tcp://0.0.0.0:9200   listen address (scheme must be tcp://)
//	-topic     news                  message topic prefix
//	-rate      1000                  messages per second
//	-size      256                   payload bytes per message
//	-dur       30s                   publish duration
//	-key-file  server_pk.hex         path to write the server public key (hex)
//	-output    ""                    path to write JSON result (stdout if empty)
//	-id        publisher             node identifier embedded in JSON result
package main

import (
	"context"
	"encoding/hex"
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
	addr    := flag.String("addr",     "tcp://0.0.0.0:9200", "TCP listen address")
	topic   := flag.String("topic",   "news",               "topic prefix for messages")
	rate    := flag.Int("rate",       1000,                  "messages per second")
	size    := flag.Int("size",       256,                   "payload bytes per message")
	dur     := flag.Duration("dur",   30*time.Second,        "publish duration")
	keyFile := flag.String("key-file", "server_pk.hex",     "path to write server public key")
	output  := flag.String("output",  "",                   "JSON result file (stdout if empty)")
	id      := flag.String("id",      "publisher",          "node identifier in JSON output")
	flag.Parse()

	serverKey, err := quicmq.GenerateCurveKey()
	if err != nil {
		log.Fatalf("generate curve key: %v", err)
	}

	pkHex := hex.EncodeToString(serverKey.Public[:])
	if err := os.WriteFile(*keyFile, []byte(pkHex), 0o644); err != nil {
		log.Fatalf("write key file %q: %v", *keyFile, err)
	}
	log.Printf("[%s] server public key → %s", *id, *keyFile)
	log.Printf("[%s] key: %s", *id, pkHex)

	ctx, cancel := context.WithTimeout(context.Background(), *dur+5*time.Second)
	defer cancel()

	pub := quicmq.NewPub(ctx, quicmq.WithCurveServer(serverKey))
	defer pub.Close()

	if err := pub.Listen(*addr); err != nil {
		log.Fatalf("[%s] listen: %v", *id, err)
	}
	log.Printf("[%s] TCP+CURVE listening on %s  topic=%q  rate=%d/s  size=%dB  dur=%s",
		*id, pub.Addr(), *topic, *rate, *size, *dur)

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
		"role":           "pub",
		"transport":      "tcp+curve",
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

func init() { _ = strings.Contains } // suppress unused import if padding removed
