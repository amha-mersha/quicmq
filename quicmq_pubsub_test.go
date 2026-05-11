package quicmq

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// isTransientRecvErr returns true for errors that indicate the underlying
// connection died — the kind of error a SUB socket with auto-reconnect
// enabled should retry over.
func isTransientRecvErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, needle := range []string{
		"EOF",
		"listener closed",
		"closed",
		"timeout",
		"no recent network activity",
		"Application error",
		"connection closed",
	} {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}

func TestPubSubBasic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Create PUB socket and listen.
	pub := NewPub(ctx)
	defer pub.Close()

	if err := pub.Listen("quic://127.0.0.1:0"); err != nil {
		t.Fatalf("pub.Listen: %v", err)
	}

	addr := pub.Addr()
	if addr == nil {
		t.Fatal("pub.Addr() returned nil")
	}
	endpoint := fmt.Sprintf("quic://%s", addr.String())
	t.Logf("PUB listening on %s", endpoint)

	// Give the listener a moment to start.
	time.Sleep(100 * time.Millisecond)

	// Create SUB socket and connect.
	sub := NewSub(ctx)
	defer sub.Close()

	if err := sub.Dial(endpoint); err != nil {
		t.Fatalf("sub.Dial: %v", err)
	}

	// Subscribe to "weather" topic.
	if err := sub.SetOption(OptionSubscribe, "weather"); err != nil {
		t.Fatalf("sub.SetOption subscribe: %v", err)
	}

	// Give the subscription time to propagate.
	time.Sleep(200 * time.Millisecond)

	// Send some messages.
	for i := range 3 {
		msg := NewMsgString(fmt.Sprintf("weather temp=%d", 20+i))
		if err := pub.Send(msg); err != nil {
			t.Fatalf("pub.Send: %v", err)
		}
	}

	// Also send a non-matching topic.
	if err := pub.Send(NewMsgString("sports goal!")); err != nil {
		t.Fatalf("pub.Send non-matching: %v", err)
	}

	// Receive messages — only "weather" ones should arrive.
	received := 0
	for received < 3 {
		msg, err := sub.Recv()
		if err != nil {
			t.Fatalf("sub.Recv: %v", err)
		}
		data := string(msg.Frames[0])
		if !strings.HasPrefix(data, "weather") {
			t.Errorf("received non-matching message: %q", data)
		}
		t.Logf("Received: %s", data)
		received++
	}
}

func TestPubSubMultipleSubscribers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pub := NewPub(ctx)
	defer pub.Close()

	if err := pub.Listen("quic://127.0.0.1:0"); err != nil {
		t.Fatalf("pub.Listen: %v", err)
	}
	endpoint := fmt.Sprintf("quic://%s", pub.Addr().String())
	time.Sleep(100 * time.Millisecond)

	const nSubs = 3
	subs := make([]Socket, nSubs)
	for i := range nSubs {
		sub := NewSub(ctx)
		defer sub.Close()
		if err := sub.Dial(endpoint); err != nil {
			t.Fatalf("sub[%d].Dial: %v", i, err)
		}
		if err := sub.SetOption(OptionSubscribe, "news"); err != nil {
			t.Fatalf("sub[%d].SetOption: %v", i, err)
		}
		subs[i] = sub
	}

	time.Sleep(300 * time.Millisecond)

	// Send messages.
	const nMsgs = 5
	for i := range nMsgs {
		if err := pub.Send(NewMsgString(fmt.Sprintf("news headline %d", i))); err != nil {
			t.Fatalf("pub.Send: %v", err)
		}
	}

	// Each subscriber should receive all messages.
	var wg sync.WaitGroup
	for si, sub := range subs {
		wg.Add(1)
		go func(idx int, s Socket) {
			defer wg.Done()
			for range nMsgs {
				msg, err := s.Recv()
				if err != nil {
					t.Errorf("sub[%d].Recv: %v", idx, err)
					return
				}
				t.Logf("sub[%d] received: %s", idx, string(msg.Frames[0]))
			}
		}(si, sub)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Log("All subscribers received all messages")
	case <-time.After(10 * time.Second):
		t.Fatal("Timed out waiting for subscribers")
	}
}

func TestPubSubReconnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// 1. Start Publisher 1
	pub1 := NewPub(ctx)
	if err := pub1.Listen("quic://127.0.0.1:9002"); err != nil {
		t.Fatalf("pub1.Listen: %v", err)
	}
	endpoint := fmt.Sprintf("quic://%s", pub1.Addr().String())

	// 2. Start Subscriber and connect
	sub := NewSub(ctx)
	defer sub.Close()
	if err := sub.Dial(endpoint); err != nil {
		t.Fatalf("sub.Dial: %v", err)
	}
	if err := sub.SetOption(OptionSubscribe, "test"); err != nil {
		t.Fatalf("sub.SetOption: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Send message to ensure it's connected
	if err := pub1.Send(NewMsgString("test first message")); err != nil {
		t.Fatalf("pub1.Send: %v", err)
	}

	msg, err := sub.Recv()
	if err != nil {
		t.Fatalf("sub.Recv: %v", err)
	}
	if string(msg.Frames[0]) != "test first message" {
		t.Fatalf("expected 'test first message', got %q", string(msg.Frames[0]))
	}

	// 3. Stop Publisher 1
	pub1.Close()
	time.Sleep(500 * time.Millisecond) // Give subscriber time to realize connection is lost and start reconnect loop

	// 4. Start Publisher 2 on the SAME endpoint
	pub2 := NewPub(ctx)
	defer pub2.Close()
	if err := pub2.Listen(endpoint); err != nil {
		t.Fatalf("pub2.Listen: %v", err)
	}

	// 5. Send message from Publisher 2. It might take a moment for the sub to reconnect.
	// Since pub2 doesn't know about sub until sub reconnects, we loop.
	// NOTE: do not call t.Logf from this goroutine — it may outlive the
	// test under -count>1, which causes a panic.
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			default:
				_ = pub2.Send(NewMsgString("test second message"))
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	t.Logf("Waiting for msg2...")
	// 6. Receive message from Publisher 2. Auto-reconnect runs in the
	// background — we retry as long as Recv keeps returning transient
	// "peer disconnected" errors. The test's ctx (20s) bounds the wait.
	var msg2 Msg
	for {
		var err error
		msg2, err = sub.Recv()
		if err != nil {
			if isTransientRecvErr(err) {
				continue
			}
			t.Fatalf("sub.Recv after reconnect: %v", err)
		}
		break
	}

	if string(msg2.Frames[0]) != "test second message" {
		t.Fatalf("expected 'test second message', got %q", string(msg2.Frames[0]))
	}
	close(done)
}
