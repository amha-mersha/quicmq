package quicmq

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestXPubXSubBasic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// XPUB listens.
	xpub := NewXPub(ctx)
	defer xpub.Close()

	if err := xpub.Listen("quic://127.0.0.1:0"); err != nil {
		t.Fatalf("xpub.Listen: %v", err)
	}
	endpoint := fmt.Sprintf("quic://%s", xpub.Addr().String())
	t.Logf("XPUB listening on %s", endpoint)
	time.Sleep(100 * time.Millisecond)

	// XSUB dials.
	xsub := NewXSub(ctx)
	defer xsub.Close()

	if err := xsub.Dial(endpoint); err != nil {
		t.Fatalf("xsub.Dial: %v", err)
	}

	// XSUB subscribes by sending a raw subscription message.
	subMsg := NewMsg(append([]byte{0x01}, "sensor"...))
	if err := xsub.Send(subMsg); err != nil {
		t.Fatalf("xsub.Send subscribe: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	// XPUB sends messages.
	for i := 0; i < 3; i++ {
		msg := NewMsgString(fmt.Sprintf("sensor value=%d", i))
		if err := xpub.Send(msg); err != nil {
			t.Fatalf("xpub.Send: %v", err)
		}
	}

	// XSUB receives.
	for i := 0; i < 3; i++ {
		msg, err := xsub.Recv()
		if err != nil {
			t.Fatalf("xsub.Recv: %v", err)
		}
		t.Logf("Received: %s", string(msg.Frames[0]))
	}
}

func TestPubXSubCompatibility(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// PUB listens.
	pub := NewPub(ctx)
	defer pub.Close()

	if err := pub.Listen("quic://127.0.0.1:0"); err != nil {
		t.Fatalf("pub.Listen: %v", err)
	}
	endpoint := fmt.Sprintf("quic://%s", pub.Addr().String())
	time.Sleep(100 * time.Millisecond)

	// XSUB connects to a regular PUB.
	xsub := NewXSub(ctx)
	defer xsub.Close()

	if err := xsub.Dial(endpoint); err != nil {
		t.Fatalf("xsub.Dial: %v", err)
	}

	// XSUB subscribes using raw message.
	if err := xsub.Send(NewMsg(append([]byte{0x01}, "data"...))); err != nil {
		t.Fatalf("xsub.Send: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	// PUB sends.
	if err := pub.Send(NewMsgString("data hello")); err != nil {
		t.Fatalf("pub.Send: %v", err)
	}

	// XSUB receives.
	msg, err := xsub.Recv()
	if err != nil {
		t.Fatalf("xsub.Recv: %v", err)
	}
	t.Logf("Received: %s", string(msg.Frames[0]))
}
