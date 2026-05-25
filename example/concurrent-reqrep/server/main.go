// concurrent-reqrep/server — REP server that handles concurrent clients and
// records throughput statistics to a JSON file.
//
// Each request is echoed back.  A background goroutine tracks the request
// rate per second and writes the final result when -dur expires.
//
// Usage:
//
//	terminal 1: go run ./example/concurrent-reqrep/server -addr quic://0.0.0.0:9400
//	terminal 2: go run ./example/concurrent-reqrep/client -workers 10 -count 200
//
// Or use the run script for fully automated multi-process tests:
//
//	./example/concurrent-reqrep/run.sh --clients 4 --workers 10
//
// Flags:
//
//	-addr    quic://0.0.0.0:9400   listen address
//	-dur     60s                    server run duration (should be > client dur)
//	-output  ""                     JSON result file (stdout if empty)
//	-id      server                 node identifier in JSON output
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	"quicmq"
)

func main() {
	addr   := flag.String("addr",   "quic://0.0.0.0:9400", "listen address")
	dur    := flag.Duration("dur",  60*time.Second,          "server run duration")
	output := flag.String("output", "",                     "JSON result file (stdout if empty)")
	id     := flag.String("id",     "server",               "node identifier in JSON output")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()

	rep := quicmq.NewRep(ctx)
	defer rep.Close()

	if err := rep.Listen(*addr); err != nil {
		log.Fatalf("[%s] listen: %v", *id, err)
	}
	log.Printf("[%s] REP server on %s  dur=%s", *id, rep.Addr(), *dur)

	var handled atomic.Int64
	var errors  atomic.Int64

	// Per-second rate reporter.
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var last int64
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				n := handled.Load()
				log.Printf("[%s] %d req/s  (total %d)", *id, n-last, n)
				last = n
			}
		}
	}()

	start := time.Now()
	for {
		msg, err := rep.Recv()
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			errors.Add(1)
			log.Printf("[%s] recv: %v", *id, err)
			continue
		}
		n := handled.Add(1)
		reply := fmt.Sprintf("pong #%d | %s", n, msg.Frames[0])
		if err := rep.Send(quicmq.NewMsgString(reply)); err != nil {
			errors.Add(1)
		}
	}

	elapsed := time.Since(start).Seconds()
	total   := handled.Load()

	result := map[string]any{
		"id":           *id,
		"role":         "rep",
		"transport":    "quic",
		"addr":         rep.Addr().String(),
		"duration_s":   elapsed,
		"reqs_handled": total,
		"errors":       errors.Load(),
		"actual_rate":  float64(total) / elapsed,
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
