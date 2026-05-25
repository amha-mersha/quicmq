package quicmq

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestConnectionPoolSharing is the core pool test: two sockets using the same
// ConnectionPool must share a single QUIC connection to the same server.
//
// The pool keeps exactly one *quic.Conn per remote address.  Both sockets open
// their own QUIC stream on that connection, so the pool's entry count stays at
// one even though two independent sockets have dialled.
func TestConnectionPoolSharing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Start a server that can accept both SUB and REP traffic.  We use a PUB
	// listener; for the test we only care that the connection is established.
	pub := NewPub(ctx)
	defer pub.Close()
	if err := pub.Listen("quic://127.0.0.1:0"); err != nil {
		t.Fatalf("pub.Listen: %v", err)
	}
	serverAddr := pub.Addr().String()
	endpoint := "quic://" + serverAddr
	t.Logf("server listening on %s", endpoint)

	pool := NewConnectionPool()
	defer pool.Close()

	// First socket dials using the pool.
	sub1 := NewSub(ctx, WithConnectionPool(pool))
	defer sub1.Close()
	if err := sub1.Dial(endpoint); err != nil {
		t.Fatalf("sub1.Dial: %v", err)
	}
	if err := sub1.SetOption(OptionSubscribe, "a"); err != nil {
		t.Fatalf("sub1.SetOption: %v", err)
	}

	// Pool should now hold exactly one entry.
	if got := pool.Len(); got != 1 {
		t.Fatalf("pool.Len() = %d after first dial, want 1", got)
	}
	localAfterFirst := pool.LocalAddr(serverAddr)
	if localAfterFirst == nil {
		t.Fatal("pool.LocalAddr returned nil after first dial")
	}
	t.Logf("shared local address: %s", localAfterFirst)

	// Second socket dials using the same pool.
	sub2 := NewSub(ctx, WithConnectionPool(pool))
	defer sub2.Close()
	if err := sub2.Dial(endpoint); err != nil {
		t.Fatalf("sub2.Dial: %v", err)
	}
	if err := sub2.SetOption(OptionSubscribe, "b"); err != nil {
		t.Fatalf("sub2.SetOption: %v", err)
	}

	// Pool must still have exactly one entry — the second dial opened a new
	// stream on the existing QUIC connection, not a new connection.
	if got := pool.Len(); got != 1 {
		t.Errorf("pool.Len() = %d after second dial, want 1 (connection not shared)", got)
	}
	localAfterSecond := pool.LocalAddr(serverAddr)
	if localAfterFirst.String() != localAfterSecond.String() {
		t.Errorf("local address changed between dials: %s → %s (new connection was created)",
			localAfterFirst, localAfterSecond)
	}
	t.Logf("verified: both sockets share the same QUIC connection (%s)", localAfterFirst)

	time.Sleep(200 * time.Millisecond)

	// Confirm both subscriptions work independently over the shared connection.
	if err := pub.Send(NewMsgString("a hello")); err != nil {
		t.Fatalf("pub.Send a: %v", err)
	}
	if err := pub.Send(NewMsgString("b world")); err != nil {
		t.Fatalf("pub.Send b: %v", err)
	}

	msg1, err := sub1.Recv()
	if err != nil {
		t.Fatalf("sub1.Recv: %v", err)
	}
	if !strings.HasPrefix(string(msg1.Frames[0]), "a") {
		t.Errorf("sub1 received wrong message: %q", msg1.Frames[0])
	}
	t.Logf("sub1 received: %q", msg1.Frames[0])

	msg2, err := sub2.Recv()
	if err != nil {
		t.Fatalf("sub2.Recv: %v", err)
	}
	if !strings.HasPrefix(string(msg2.Frames[0]), "b") {
		t.Errorf("sub2 received wrong message: %q", msg2.Frames[0])
	}
	t.Logf("sub2 received: %q", msg2.Frames[0])
}

// TestConnectionPoolMixedPatterns demonstrates that heterogeneous socket types
// (PUB subscriber + REQ client) can share a QUIC connection to different
// servers via independent pool entries.
func TestConnectionPoolMixedPatterns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// PUB server.
	pub := NewPub(ctx)
	defer pub.Close()
	if err := pub.Listen("quic://127.0.0.1:0"); err != nil {
		t.Fatalf("pub.Listen: %v", err)
	}
	pubEndpoint := fmt.Sprintf("quic://%s", pub.Addr().String())

	// REP server.
	rep := NewRep(ctx)
	defer rep.Close()
	if err := rep.Listen("quic://127.0.0.1:0"); err != nil {
		t.Fatalf("rep.Listen: %v", err)
	}
	repEndpoint := fmt.Sprintf("quic://%s", rep.Addr().String())
	go func() {
		for {
			msg, err := rep.Recv()
			if err != nil {
				return
			}
			_ = rep.Send(NewMsgString("pong:" + string(msg.Frames[0])))
		}
	}()

	pool := NewConnectionPool()
	defer pool.Close()

	// SUB dialling the pub server.
	sub := NewSub(ctx, WithConnectionPool(pool))
	defer sub.Close()
	if err := sub.Dial(pubEndpoint); err != nil {
		t.Fatalf("sub.Dial: %v", err)
	}
	if err := sub.SetOption(OptionSubscribe, "ping"); err != nil {
		t.Fatalf("sub.SetOption: %v", err)
	}

	// REQ dialling the rep server (different remote addr → separate pool entry).
	req := NewReq(ctx, WithConnectionPool(pool))
	defer req.Close()
	if err := req.Dial(repEndpoint); err != nil {
		t.Fatalf("req.Dial: %v", err)
	}

	// Two distinct remote addresses → two pool entries.
	if got := pool.Len(); got != 2 {
		t.Fatalf("pool.Len() = %d, want 2 (one per distinct server)", got)
	}
	t.Logf("pool holds %d connections (one per distinct server)", pool.Len())

	time.Sleep(200 * time.Millisecond)

	// PUB/SUB round.
	if err := pub.Send(NewMsgString("ping world")); err != nil {
		t.Fatalf("pub.Send: %v", err)
	}
	msg, err := sub.Recv()
	if err != nil {
		t.Fatalf("sub.Recv: %v", err)
	}
	t.Logf("SUB received: %q", msg.Frames[0])

	// REQ/REP round.
	if err := req.Send(NewMsgString("hello")); err != nil {
		t.Fatalf("req.Send: %v", err)
	}
	reply, err := req.Recv()
	if err != nil {
		t.Fatalf("req.Recv: %v", err)
	}
	t.Logf("REQ received: %q", reply.Frames[0])
}

// TestConnectionPoolClose verifies that closing the pool closes all cached
// connections cleanly.
func TestConnectionPoolClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pub := NewPub(ctx)
	defer pub.Close()
	if err := pub.Listen("quic://127.0.0.1:0"); err != nil {
		t.Fatalf("pub.Listen: %v", err)
	}
	endpoint := fmt.Sprintf("quic://%s", pub.Addr().String())

	pool := NewConnectionPool()

	sub := NewSub(ctx, WithConnectionPool(pool))
	defer sub.Close()
	if err := sub.Dial(endpoint); err != nil {
		t.Fatalf("sub.Dial: %v", err)
	}

	if got := pool.Len(); got != 1 {
		t.Fatalf("pool.Len() = %d, want 1", got)
	}

	if err := pool.Close(); err != nil {
		t.Fatalf("pool.Close: %v", err)
	}

	if got := pool.Len(); got != 0 {
		t.Errorf("pool.Len() = %d after Close, want 0", got)
	}
	t.Log("pool closed cleanly")
}
