package quicmq

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// ── TCP comparison benchmarks ─────────────────────────────────────────────────
// These mirror the QUIC benchmarks above but use the TCP transport so that
// the thesis can present a direct side-by-side comparison.

func BenchmarkReqRepLatencyTCP(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rep := NewRep(ctx)
	defer rep.Close()
	if err := rep.Listen("tcp://127.0.0.1:0"); err != nil {
		b.Fatalf("rep.Listen: %v", err)
	}
	go func() {
		for {
			msg, err := rep.Recv()
			if err != nil {
				return
			}
			_ = rep.Send(msg)
		}
	}()

	req := NewReq(ctx)
	defer req.Close()
	if err := req.Dial(fmt.Sprintf("tcp://%s", rep.Addr())); err != nil {
		b.Fatalf("req.Dial: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	payload := NewMsgString("ping")

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		if err := req.Send(payload); err != nil {
			b.Fatalf("req.Send: %v", err)
		}
		if _, err := req.Recv(); err != nil {
			b.Fatalf("req.Recv: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N), "ns/rtt")
}

func BenchmarkPubSubThroughputTCP(b *testing.B) {
	for _, size := range []int{64, 1024, 8192} {
		b.Run(fmt.Sprintf("msg=%dB", size), func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			pub := NewPub(ctx)
			defer pub.Close()
			if err := pub.Listen("tcp://127.0.0.1:0"); err != nil {
				b.Fatalf("pub.Listen: %v", err)
			}

			sub := NewSub(ctx)
			defer sub.Close()
			if err := sub.Dial(fmt.Sprintf("tcp://%s", pub.Addr())); err != nil {
				b.Fatalf("sub.Dial: %v", err)
			}
			if err := sub.SetOption(OptionSubscribe, ""); err != nil {
				b.Fatalf("sub.SetOption: %v", err)
			}
			time.Sleep(100 * time.Millisecond)

			payload := NewMsg(make([]byte, size))

			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					default:
						_ = pub.Send(payload)
					}
				}
			}()

			b.SetBytes(int64(size))
			b.ResetTimer()
			b.ReportAllocs()

			for range b.N {
				if _, err := sub.Recv(); err != nil {
					b.Fatalf("sub.Recv: %v", err)
				}
			}
		})
	}
}

// BenchmarkReqRepLatency measures the end-to-end round-trip latency of the
// REQ/REP pattern over QUIC.  Each iteration sends one request and waits for
// the reply — this models the latency a caller experiences in synchronous RPC.
func BenchmarkReqRepLatency(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rep := NewRep(ctx)
	defer rep.Close()
	if err := rep.Listen("quic://127.0.0.1:0"); err != nil {
		b.Fatalf("rep.Listen: %v", err)
	}
	go func() {
		for {
			msg, err := rep.Recv()
			if err != nil {
				return
			}
			_ = rep.Send(msg) // echo back
		}
	}()

	req := NewReq(ctx)
	defer req.Close()
	if err := req.Dial(fmt.Sprintf("quic://%s", rep.Addr())); err != nil {
		b.Fatalf("req.Dial: %v", err)
	}
	time.Sleep(50 * time.Millisecond) // let the connection settle

	payload := NewMsgString("ping")

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		if err := req.Send(payload); err != nil {
			b.Fatalf("req.Send: %v", err)
		}
		if _, err := req.Recv(); err != nil {
			b.Fatalf("req.Recv: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N), "ns/rtt")
}

// BenchmarkPubSubThroughput measures publish–subscribe message throughput.
// The publisher runs continuously in the background; the subscriber consumes
// exactly b.N messages.  This avoids flow-control stalls from sending a large
// burst before the subscriber is ready.
func BenchmarkPubSubThroughput(b *testing.B) {
	for _, size := range []int{64, 1024, 8192} {
		b.Run(fmt.Sprintf("msg=%dB", size), func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			pub := NewPub(ctx)
			defer pub.Close()
			if err := pub.Listen("quic://127.0.0.1:0"); err != nil {
				b.Fatalf("pub.Listen: %v", err)
			}

			sub := NewSub(ctx)
			defer sub.Close()
			if err := sub.Dial(fmt.Sprintf("quic://%s", pub.Addr())); err != nil {
				b.Fatalf("sub.Dial: %v", err)
			}
			if err := sub.SetOption(OptionSubscribe, ""); err != nil {
				b.Fatalf("sub.SetOption: %v", err)
			}
			time.Sleep(100 * time.Millisecond)

			payload := NewMsg(make([]byte, size))

			// Publisher runs continuously — the subscriber controls pacing.
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					default:
						_ = pub.Send(payload)
					}
				}
			}()

			b.SetBytes(int64(size))
			b.ResetTimer()
			b.ReportAllocs()

			for range b.N {
				if _, err := sub.Recv(); err != nil {
					b.Fatalf("sub.Recv: %v", err)
				}
			}
		})
	}
}

// BenchmarkDatagramThroughput measures throughput of the DatagramPub/Sub
// socket type using QUIC RFC 9221 unreliable datagrams.  Comparing this
// benchmark against BenchmarkPubSubThroughput shows the latency/throughput
// trade-off between reliable streams and datagrams.
func BenchmarkDatagramThroughput(b *testing.B) {
	for _, size := range []int{64, 1024} {
		b.Run(fmt.Sprintf("msg=%dB", size), func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			pub := NewDatagramPub(ctx)
			defer pub.Close()
			if err := pub.Listen("quic://127.0.0.1:0"); err != nil {
				b.Fatalf("pub.Listen: %v", err)
			}

			sub := NewDatagramSub(ctx)
			defer sub.Close()
			if err := sub.Dial(fmt.Sprintf("quic://%s", pub.Addr())); err != nil {
				b.Fatalf("sub.Dial: %v", err)
			}
			if err := sub.SetOption(OptionSubscribe, ""); err != nil {
				b.Fatalf("sub.SetOption: %v", err)
			}
			time.Sleep(150 * time.Millisecond)

			payload := NewMsg(make([]byte, size))

			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					default:
						_ = pub.Send(payload)
					}
				}
			}()

			b.SetBytes(int64(size))
			b.ResetTimer()
			b.ReportAllocs()

			for range b.N {
				if _, err := sub.Recv(); err != nil {
					b.Fatalf("sub.Recv: %v", err)
				}
			}
		})
	}
}

// BenchmarkConnectionPool measures the overhead of dialling through a
// ConnectionPool versus a fresh connection.  The pooled case reuses an
// existing QUIC connection; the un-pooled case dials a new one each time.
func BenchmarkConnectionPool(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pub := NewPub(ctx)
	defer pub.Close()
	if err := pub.Listen("quic://127.0.0.1:0"); err != nil {
		b.Fatalf("pub.Listen: %v", err)
	}
	endpoint := fmt.Sprintf("quic://%s", pub.Addr())

	b.Run("pooled", func(b *testing.B) {
		pool := NewConnectionPool()
		defer pool.Close()

		b.ResetTimer()
		b.ReportAllocs()
		for range b.N {
			sub := NewSub(ctx, WithConnectionPool(pool))
			if err := sub.Dial(endpoint); err != nil {
				b.Fatalf("Dial: %v", err)
			}
			sub.Close()
		}
	})

	b.Run("unpooled", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for range b.N {
			sub := NewSub(ctx)
			if err := sub.Dial(endpoint); err != nil {
				b.Fatalf("Dial: %v", err)
			}
			sub.Close()
		}
	})
}

// BenchmarkReconnectTime measures how quickly a subscriber can reconnect to a
// restarted publisher.  The result is the mean time from publisher restart to
// first message received, which captures QUIC handshake overhead (and 0-RTT
// session resumption benefit when the session cache is warm).
func BenchmarkReconnectTime(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use a fixed port so pub2 can restart on the same address.
	const addr = "quic://127.0.0.1:19872"

	pub1 := NewPub(ctx)
	if err := pub1.Listen(addr); err != nil {
		b.Fatalf("pub1.Listen: %v", err)
	}

	sub := NewSub(ctx)
	defer sub.Close()
	if err := sub.Dial(addr); err != nil {
		b.Fatalf("sub.Dial: %v", err)
	}
	if err := sub.SetOption(OptionSubscribe, "bench"); err != nil {
		b.Fatalf("sub.SetOption: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	// Prime the session cache: send one message so the handshake completes
	// and the TLS session ticket is stored.
	if err := pub1.Send(NewMsgString("bench prime")); err != nil {
		b.Fatalf("prime send: %v", err)
	}
	if _, err := sub.Recv(); err != nil {
		b.Fatalf("prime recv: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	pub1.Close()
	time.Sleep(100 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()

	var totalReconnect time.Duration
	for i := range b.N {
		pub := NewPub(ctx)
		if err := pub.Listen(addr); err != nil {
			b.Fatalf("iter %d pub.Listen: %v", i, err)
		}

		start := time.Now()
		stop := make(chan struct{})
		go func() {
			for {
				select {
				case <-stop:
					return
				case <-ctx.Done():
					return
				default:
					_ = pub.Send(NewMsgString("bench msg"))
					time.Sleep(10 * time.Millisecond)
				}
			}
		}()

		for {
			msg, err := sub.Recv()
			if err != nil {
				continue
			}
			if string(msg.Frames[0]) == "bench msg" {
				totalReconnect += time.Since(start)
				break
			}
		}

		close(stop)
		pub.Close()
		time.Sleep(100 * time.Millisecond)
	}
	b.StopTimer()

	if b.N > 0 {
		meanMs := float64(totalReconnect.Milliseconds()) / float64(b.N)
		b.ReportMetric(meanMs, "ms/reconnect")
	}
}
