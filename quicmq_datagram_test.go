package quicmq

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDatagramPubSubBasic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pub := NewDatagramPub(ctx)
	if err := pub.Listen("quic://127.0.0.1:0"); err != nil {
		t.Fatalf("pub.Listen: %v", err)
	}
	defer pub.Close()

	endpoint := fmt.Sprintf("quic://%s", pub.Addr().String())
	t.Logf("DatagramPub listening on %s", endpoint)

	sub := NewDatagramSub(ctx)
	defer sub.Close()

	if err := sub.Dial(endpoint); err != nil {
		t.Fatalf("sub.Dial: %v", err)
	}
	if err := sub.SetOption(OptionSubscribe, "sensor"); err != nil {
		t.Fatalf("sub.SetOption: %v", err)
	}

	// Let subscription propagate to the publisher.
	time.Sleep(200 * time.Millisecond)

	const nMsgs = 5
	for i := range nMsgs {
		if err := pub.Send(NewMsgString(fmt.Sprintf("sensor temp=%d", 20+i))); err != nil {
			t.Fatalf("pub.Send %d: %v", i, err)
		}
	}
	// Also send a non-matching message to ensure filtering works.
	if err := pub.Send(NewMsgString("video frame")); err != nil {
		t.Fatalf("pub.Send non-matching: %v", err)
	}

	for i := range nMsgs {
		msg, err := sub.Recv()
		if err != nil {
			t.Fatalf("sub.Recv %d: %v", i, err)
		}
		data := string(msg.Frames[0])
		if !strings.HasPrefix(data, "sensor") {
			t.Errorf("received non-matching message: %q", data)
		}
		t.Logf("received datagram: %q", data)
	}
}

func TestDatagramTopicFiltering(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pub := NewDatagramPub(ctx)
	if err := pub.Listen("quic://127.0.0.1:0"); err != nil {
		t.Fatalf("pub.Listen: %v", err)
	}
	defer pub.Close()

	endpoint := fmt.Sprintf("quic://%s", pub.Addr().String())

	// subA subscribes to "temperature", subB to "humidity".
	subA := NewDatagramSub(ctx)
	defer subA.Close()
	if err := subA.Dial(endpoint); err != nil {
		t.Fatalf("subA.Dial: %v", err)
	}
	if err := subA.SetOption(OptionSubscribe, "temperature"); err != nil {
		t.Fatalf("subA.SetOption: %v", err)
	}

	subB := NewDatagramSub(ctx)
	defer subB.Close()
	if err := subB.Dial(endpoint); err != nil {
		t.Fatalf("subB.Dial: %v", err)
	}
	if err := subB.SetOption(OptionSubscribe, "humidity"); err != nil {
		t.Fatalf("subB.SetOption: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	messages := []string{
		"temperature 22.1",
		"humidity 65%",
		"temperature 22.5",
		"humidity 67%",
	}
	for _, m := range messages {
		if err := pub.Send(NewMsgString(m)); err != nil {
			t.Fatalf("pub.Send %q: %v", m, err)
		}
	}

	var wg sync.WaitGroup
	checkSub := func(name string, sub *DatagramSubSocket, prefix string, want int) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got := 0
			recvCtx, recvCancel := context.WithTimeout(ctx, 5*time.Second)
			defer recvCancel()
			for got < want {
				select {
				case <-recvCtx.Done():
					t.Errorf("%s: timeout after %d/%d messages", name, got, want)
					return
				default:
				}
				msg, err := sub.Recv()
				if err != nil {
					if recvCtx.Err() != nil {
						t.Errorf("%s: timeout after %d/%d messages", name, got, want)
					} else {
						t.Errorf("%s: Recv: %v", name, err)
					}
					return
				}
				data := string(msg.Frames[0])
				if !strings.HasPrefix(data, prefix) {
					t.Errorf("%s: unexpected message %q", name, data)
				}
				t.Logf("%s received: %q", name, data)
				got++
			}
		}()
	}

	checkSub("subA", subA, "temperature", 2)
	checkSub("subB", subB, "humidity", 2)
	wg.Wait()
}

func TestDatagramTopics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sub := NewDatagramSub(ctx)
	defer sub.Close()

	_ = sub.SetOption(OptionSubscribe, "alpha")
	_ = sub.SetOption(OptionSubscribe, "beta")
	_ = sub.SetOption(OptionSubscribe, "gamma")

	topics := sub.Topics()
	if len(topics) != 3 {
		t.Fatalf("expected 3 topics, got %d: %v", len(topics), topics)
	}
	// Topics() must be sorted.
	if topics[0] != "alpha" || topics[1] != "beta" || topics[2] != "gamma" {
		t.Errorf("unexpected topic order: %v", topics)
	}
	t.Logf("Topics: %v", topics)

	_ = sub.SetOption(OptionUnsubscribe, "beta")
	topics = sub.Topics()
	if len(topics) != 2 {
		t.Fatalf("expected 2 topics after unsubscribe, got %d: %v", len(topics), topics)
	}
}

func TestDatagramSocketTypes(t *testing.T) {
	ctx := context.Background()
	pub := NewDatagramPub(ctx)
	defer pub.Close()
	sub := NewDatagramSub(ctx)
	defer sub.Close()

	if pub.Type() != DatagramPub {
		t.Errorf("pub.Type() = %q, want %q", pub.Type(), DatagramPub)
	}
	if sub.Type() != DatagramSub {
		t.Errorf("sub.Type() = %q, want %q", sub.Type(), DatagramSub)
	}
	if !DatagramPub.IsCompatible(DatagramSub) {
		t.Error("DatagramPub should be compatible with DatagramSub")
	}
	if !DatagramSub.IsCompatible(DatagramPub) {
		t.Error("DatagramSub should be compatible with DatagramPub")
	}
	if DatagramPub.IsCompatible(Sub) {
		t.Error("DatagramPub should NOT be compatible with Sub")
	}
}
