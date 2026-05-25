package quicmq

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// sharedCurveKeys generates a server keypair and a matching client keypair
// for use across CURVE tests in this file.
func sharedCurveKeys(t *testing.T) (serverKey, clientKey CurveKey) {
	t.Helper()
	var err error
	serverKey, err = GenerateCurveKey()
	if err != nil {
		t.Fatalf("GenerateCurveKey (server): %v", err)
	}
	clientKey, err = GenerateCurveKey()
	if err != nil {
		t.Fatalf("GenerateCurveKey (client): %v", err)
	}
	return
}

func TestCURVETCPReqRep(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	serverKey, clientKey := sharedCurveKeys(t)

	rep := NewRep(ctx, WithCurveServer(serverKey))
	if err := rep.Listen("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("rep.Listen: %v", err)
	}
	defer rep.Close()
	ep := fmt.Sprintf("tcp://%s", rep.Addr().String())
	t.Logf("REP listening on %s (CURVE)", ep)

	go func() {
		for {
			msg, err := rep.Recv()
			if err != nil {
				return
			}
			_ = rep.Send(NewMsgString("pong:" + string(msg.Frames[0])))
		}
	}()

	req := NewReq(ctx, WithCurveClient(clientKey, serverKey.Public))
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

func TestCURVETCPPubSubBasic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	serverKey, clientKey := sharedCurveKeys(t)

	pub := NewPub(ctx, WithCurveServer(serverKey))
	if err := pub.Listen("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("pub.Listen: %v", err)
	}
	defer pub.Close()
	ep := fmt.Sprintf("tcp://%s", pub.Addr().String())

	sub := NewSub(ctx, WithCurveClient(clientKey, serverKey.Public))
	defer sub.Close()
	if err := sub.Dial(ep); err != nil {
		t.Fatalf("sub.Dial: %v", err)
	}
	if err := sub.SetOption(OptionSubscribe, "sensor"); err != nil {
		t.Fatalf("sub.SetOption: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	const n = 5
	for i := range n {
		if err := pub.Send(NewMsgString(fmt.Sprintf("sensor temp=%d", 20+i))); err != nil {
			t.Fatalf("pub.Send %d: %v", i, err)
		}
	}
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

func TestCURVETCPPubSubFanOut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	serverKey, clientKey := sharedCurveKeys(t)

	pub := NewPub(ctx, WithCurveServer(serverKey))
	if err := pub.Listen("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("pub.Listen: %v", err)
	}
	defer pub.Close()
	ep := fmt.Sprintf("tcp://%s", pub.Addr().String())

	const nsubs = 3
	subs := make([]Socket, nsubs)
	for i := range subs {
		// Each subscriber uses the same server public key but its own keypair.
		ck, err := GenerateCurveKey()
		if err != nil {
			t.Fatalf("GenerateCurveKey sub[%d]: %v", i, err)
		}
		_ = clientKey // suppress unused warning; each sub has its own key
		s := NewSub(ctx, WithCurveClient(ck, serverKey.Public))
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

func TestCURVETCPMultiFrame(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	serverKey, clientKey := sharedCurveKeys(t)

	rep := NewRep(ctx, WithCurveServer(serverKey))
	if err := rep.Listen("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("rep.Listen: %v", err)
	}
	defer rep.Close()
	ep := fmt.Sprintf("tcp://%s", rep.Addr().String())

	go func() {
		msg, err := rep.Recv()
		if err != nil {
			return
		}
		_ = rep.SendMulti(msg)
	}()

	req := NewReq(ctx, WithCurveClient(clientKey, serverKey.Public))
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

// TestCURVEWrongKey verifies that a client with an incorrect server public key
// is rejected during the WELCOME decryption step.
func TestCURVEWrongKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	serverKey, clientKey := sharedCurveKeys(t)
	wrongKey, _ := GenerateCurveKey() // different key — should not authenticate

	rep := NewRep(ctx, WithCurveServer(serverKey))
	if err := rep.Listen("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("rep.Listen: %v", err)
	}
	defer rep.Close()
	ep := fmt.Sprintf("tcp://%s", rep.Addr().String())

	// Client uses the wrong server public key — handshake must fail.
	req := NewReq(ctx,
		WithCurveClient(clientKey, wrongKey.Public),
		WithDialerMaxRetries(0),
	)
	defer req.Close()
	if err := req.Dial(ep); err == nil {
		t.Fatal("Dial with wrong server key should have failed")
	} else {
		t.Logf("correctly rejected: %v", err)
	}
}
