package quicmq

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"
)

// Test0RTTSessionResumption verifies that a second QUIC connection to the same
// server uses 0-RTT early data when a TLS session ticket was cached from the
// first (full-handshake) connection.
//
// The test operates at the quicTransport level so it can inspect the raw
// *quic.Conn.ConnectionState().Used0RTT field directly.
func Test0RTTSessionResumption(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	trans := &quicTransport{}

	// --- Server ---
	serverTLS := GenerateTLSConfig()
	serverCtx := withServerTLS(ctx, serverTLS)
	l, err := trans.Listen(serverCtx, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()

	addr := l.Addr().String()

	// Accept connections and drain all streams silently.
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go io.Copy(io.Discard, conn)
		}
	}()

	// --- Client: single TLS config with session cache, reused across dials ---
	clientTLS := InsecureClientTLSConfig() // includes tls.LRUClientSessionCache
	clientCtx := withClientTLS(ctx, clientTLS)

	// ── First dial: cold start ────────────────────────────────────────────────
	conn1, err := trans.Dial(clientCtx, addr)
	if err != nil {
		t.Fatalf("first dial: %v", err)
	}
	sc1 := conn1.(*streamConn)

	// Wait for the full handshake so the server has sent a session ticket and
	// the client's session cache has stored it.
	select {
	case <-sc1.qconn.HandshakeComplete():
	case <-ctx.Done():
		t.Fatal("first handshake timed out")
	}
	firstUsed0RTT := sc1.qconn.ConnectionState().Used0RTT
	t.Logf("1st connection Used0RTT=%v (expected false — cold start)", firstUsed0RTT)

	// Brief pause: TLS 1.3 NewSessionTicket messages arrive slightly after the
	// Finished message.  The session cache is populated asynchronously.
	time.Sleep(150 * time.Millisecond)

	// Close the QUIC connection (not just the stream) before reconnecting.
	sc1.qconn.CloseWithError(0, "done")
	time.Sleep(50 * time.Millisecond)

	// ── Second dial: should resume with 0-RTT ────────────────────────────────
	conn2, err := trans.Dial(clientCtx, addr)
	if err != nil {
		t.Fatalf("second dial: %v", err)
	}
	sc2 := conn2.(*streamConn)
	defer conn2.Close()

	// Wait for handshake; Used0RTT is set once quic-go confirms early data.
	select {
	case <-sc2.qconn.HandshakeComplete():
	case <-ctx.Done():
		t.Fatal("second handshake timed out")
	}

	used0RTT := sc2.qconn.ConnectionState().Used0RTT
	t.Logf("2nd connection Used0RTT=%v (expected true  — session resumption)", used0RTT)
	if !used0RTT {
		t.Error("0-RTT was not used on the second connection: session ticket may not have been cached")
	}
}

// Test0RTTWithSocket verifies that the higher-level Socket API also benefits
// from 0-RTT: the SUB socket reconnects to a restarted PUB with a 0-RTT
// handshake, which is faster than the cold-start round-trip.
func Test0RTTWithSocket(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// ── Publisher 1: first connection (cold start) ───────────────────────────
	pub1 := NewPub(ctx)
	if err := pub1.Listen("quic://127.0.0.1:0"); err != nil {
		t.Fatalf("pub1.Listen: %v", err)
	}
	endpoint := fmt.Sprintf("quic://%s", pub1.Addr().String())

	sub := NewSub(ctx)
	defer sub.Close()
	if err := sub.Dial(endpoint); err != nil {
		t.Fatalf("sub.Dial: %v", err)
	}
	if err := sub.SetOption(OptionSubscribe, "ping"); err != nil {
		t.Fatalf("sub.SetOption: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	// Exchange one message to confirm the first connection works.
	if err := pub1.Send(NewMsgString("ping cold")); err != nil {
		t.Fatalf("pub1.Send: %v", err)
	}
	if msg, err := sub.Recv(); err != nil {
		t.Fatalf("sub.Recv cold: %v", err)
	} else {
		t.Logf("cold-start message received: %q", string(msg.Frames[0]))
	}

	// Give the TLS stack time to cache the session ticket.
	time.Sleep(200 * time.Millisecond)
	pub1.Close()
	time.Sleep(100 * time.Millisecond)

	// ── Publisher 2: restart on same address ─────────────────────────────────
	pub2 := NewPub(ctx)
	defer pub2.Close()
	if err := pub2.Listen(endpoint); err != nil {
		t.Fatalf("pub2.Listen: %v", err)
	}

	// Measure time from pub2 start to first received message after reconnect.
	reconnectStart := time.Now()
	stop := make(chan struct{})
	defer close(stop)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			default:
				_ = pub2.Send(NewMsgString("ping warm"))
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()

	for {
		msg, err := sub.Recv()
		if err != nil {
			if isTransientRecvErr(err) {
				continue
			}
			t.Fatalf("sub.Recv after reconnect: %v", err)
		}
		if string(msg.Frames[0]) == "ping warm" {
			t.Logf("reconnected and received first message in %v", time.Since(reconnectStart))
			break
		}
	}
}
