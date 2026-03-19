package quicmq

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

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
	for i := 0; i < 3; i++ {
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
	for i := 0; i < nSubs; i++ {
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
	for i := 0; i < nMsgs; i++ {
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
			for j := 0; j < nMsgs; j++ {
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
