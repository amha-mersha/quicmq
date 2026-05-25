package quicmq

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// endpoint returns a tcp://127.0.0.1:PORT string from a listener address.
func tcpEndpoint(addr string) string {
	return fmt.Sprintf("tcp://%s", addr)
}

func TestTCPPubSubBasic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pub := NewPub(ctx)
	if err := pub.Listen("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("pub.Listen: %v", err)
	}
	defer pub.Close()

	ep := tcpEndpoint(pub.Addr().String())
	t.Logf("PUB listening on %s", ep)

	sub := NewSub(ctx)
	defer sub.Close()

	if err := sub.Dial(ep); err != nil {
		t.Fatalf("sub.Dial: %v", err)
	}
	if err := sub.SetOption(OptionSubscribe, "sensor"); err != nil {
		t.Fatalf("sub.SetOption: %v", err)
	}

	// Let subscription propagate.
	time.Sleep(100 * time.Millisecond)

	const n = 5
	for i := range n {
		if err := pub.Send(NewMsgString(fmt.Sprintf("sensor temp=%d", 20+i))); err != nil {
			t.Fatalf("pub.Send %d: %v", i, err)
		}
	}
	// Send a non-matching message to verify filtering.
	if err := pub.Send(NewMsgString("video frame1")); err != nil {
		t.Fatalf("pub.Send non-match: %v", err)
	}

	for i := range n {
		msg, err := sub.Recv()
		if err != nil {
			t.Fatalf("sub.Recv %d: %v", i, err)
		}
		data := string(msg.Frames[0])
		if !strings.HasPrefix(data, "sensor") {
			t.Errorf("recv %d: unexpected message %q", i, data)
		}
		t.Logf("recv[%d]: %q", i, data)
	}
}

func TestTCPPubSubFanOut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pub := NewPub(ctx)
	if err := pub.Listen("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("pub.Listen: %v", err)
	}
	defer pub.Close()
	ep := tcpEndpoint(pub.Addr().String())

	const nsubs = 3
	subs := make([]Socket, nsubs)
	for i := range subs {
		s := NewSub(ctx)
		defer s.Close()
		if err := s.Dial(ep); err != nil {
			t.Fatalf("sub[%d].Dial: %v", i, err)
		}
		if err := s.SetOption(OptionSubscribe, ""); err != nil {
			t.Fatalf("sub[%d].SetOption: %v", i, err)
		}
		subs[i] = s
	}
	time.Sleep(150 * time.Millisecond)

	const nmsgs = 4
	for i := range nmsgs {
		if err := pub.Send(NewMsgString(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatalf("pub.Send %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	for i, s := range subs {
		wg.Add(1)
		go func(idx int, sub Socket) {
			defer wg.Done()
			for j := range nmsgs {
				msg, err := sub.Recv()
				if err != nil {
					t.Errorf("sub[%d] Recv %d: %v", idx, j, err)
					return
				}
				t.Logf("sub[%d] recv: %q", idx, string(msg.Frames[0]))
			}
		}(i, s)
	}
	wg.Wait()
}

func TestTCPReqRep(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	rep := NewRep(ctx)
	if err := rep.Listen("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("rep.Listen: %v", err)
	}
	defer rep.Close()
	ep := tcpEndpoint(rep.Addr().String())

	// Server goroutine
	go func() {
		for {
			msg, err := rep.Recv()
			if err != nil {
				return
			}
			_ = rep.Send(NewMsgString("pong:" + string(msg.Frames[0])))
		}
	}()

	req := NewReq(ctx)
	defer req.Close()
	if err := req.Dial(ep); err != nil {
		t.Fatalf("req.Dial: %v", err)
	}

	for i := range 10 {
		payload := fmt.Sprintf("ping-%d", i)
		if err := req.Send(NewMsgString(payload)); err != nil {
			t.Fatalf("req.Send %d: %v", i, err)
		}
		reply, err := req.Recv()
		if err != nil {
			t.Fatalf("req.Recv %d: %v", i, err)
		}
		got := string(reply.Frames[0])
		want := "pong:" + payload
		if got != want {
			t.Errorf("round %d: got %q, want %q", i, got, want)
		}
		t.Logf("round %d: %q", i, got)
	}
}

func TestTCPMultiFrame(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rep := NewRep(ctx)
	if err := rep.Listen("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("rep.Listen: %v", err)
	}
	defer rep.Close()
	ep := tcpEndpoint(rep.Addr().String())

	go func() {
		msg, err := rep.Recv()
		if err != nil {
			return
		}
		// Echo all frames back.
		_ = rep.SendMulti(msg)
	}()

	req := NewReq(ctx)
	defer req.Close()
	if err := req.Dial(ep); err != nil {
		t.Fatalf("req.Dial: %v", err)
	}

	sent := NewMsgFrom([]byte("frame-a"), []byte("frame-b"), []byte("frame-c"))
	if err := req.SendMulti(sent); err != nil {
		t.Fatalf("req.SendMulti: %v", err)
	}
	recv, err := req.Recv()
	if err != nil {
		t.Fatalf("req.Recv: %v", err)
	}
	if len(recv.Frames) != 3 {
		t.Fatalf("want 3 frames, got %d", len(recv.Frames))
	}
	for i, frame := range recv.Frames {
		t.Logf("frame[%d]: %q", i, frame)
	}
}

func TestTCPTransportRegistered(t *testing.T) {
	found := false
	for _, name := range Transports() {
		if name == "tcp" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("tcp transport not registered; registered: %v", Transports())
	}
}
