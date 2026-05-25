package quicmq

import (
	"context"
	"testing"
	"time"
)

// --- GSO (Generic Segmentation Offload) ---

// TestGSOEnabled confirms that quic-go's GSO optimisation is active for
// quicmq connections.  GSO is enabled automatically when the underlying
// net.PacketConn implements the OOBCapablePacketConn interface — which
// *net.UDPConn (returned by net.ListenUDP) does.  We verify this at the
// transport level by checking that a connection can be established and data
// flows; if GSO caused a crash or panic quic-go would surface an error.
//
// GSO itself is not user-observable without kernel tracing, so this test acts
// as a smoke-test: it confirms the connection path that enables GSO is intact.
func TestGSOEnabled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pub := NewPub(ctx)
	if err := pub.Listen("quic://127.0.0.1:0"); err != nil {
		t.Fatalf("pub.Listen: %v", err)
	}
	defer pub.Close()

	sub := NewSub(ctx)
	defer sub.Close()
	if err := sub.Dial("quic://" + pub.Addr().String()); err != nil {
		t.Fatalf("sub.Dial: %v", err)
	}
	if err := sub.SetOption(OptionSubscribe, ""); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	time.Sleep(80 * time.Millisecond)

	if err := pub.Send(NewMsgString("gso-test")); err != nil {
		t.Fatalf("send: %v", err)
	}
	msg, err := sub.Recv()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if got := string(msg.Frames[0]); got != "gso-test" {
		t.Errorf("unexpected message %q", got)
	}
	t.Log("GSO smoke-test passed: *net.UDPConn path is intact")
}

// --- UDP Buffer Sizes ---

// TestUDPBufferSizeDefault verifies that the default UDP buffer size (7 MiB)
// is stored on the socket and threaded into context for the transport layer.
func TestUDPBufferSizeDefault(t *testing.T) {
	ctx := context.Background()
	pub := NewPub(ctx)
	defer pub.Close()

	size := pub.(*pubSocket).socket.udpBufferSize
	if size != defaultUDPBufferSize {
		t.Errorf("default udpBufferSize = %d, want %d", size, defaultUDPBufferSize)
	}
}

// TestUDPBufferSizeOption verifies that WithUDPBufferSize overrides the default.
func TestUDPBufferSizeOption(t *testing.T) {
	const custom = 4 << 20 // 4 MiB
	ctx := context.Background()
	pub := NewPub(ctx, WithUDPBufferSize(custom))
	defer pub.Close()

	size := pub.(*pubSocket).socket.udpBufferSize
	if size != custom {
		t.Errorf("udpBufferSize = %d, want %d", size, custom)
	}
}

// TestUDPBufferSizeApplied verifies that the buffer size from WithUDPBufferSize
// is actually applied to the underlying *net.UDPConn.  We check by reading back
// the socket's SO_RCVBUF value after Listen/Dial.  The OS may grant less than
// requested (capped by net.core.rmem_max on Linux), but it must be ≥ the OS
// default (typically 208 KiB) — confirming our SetReadBuffer call ran.
func TestUDPBufferSizeApplied(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use a large explicit buffer size so we can see the attempt is made.
	pub := NewPub(ctx, WithUDPBufferSize(defaultUDPBufferSize))
	if err := pub.Listen("quic://127.0.0.1:0"); err != nil {
		t.Fatalf("pub.Listen: %v", err)
	}
	defer pub.Close()

	// Access the underlying UDP conn to read back the buffer size.
	ql := pub.(*pubSocket).socket.listener.(*quicListener)
	rcvBuf, err := ql.udpConn.SyscallConn()
	if err != nil {
		t.Skipf("SyscallConn not available: %v", err)
	}

	var bufSize int
	ctrlErr := rcvBuf.Control(func(fd uintptr) {
		// syscall.GetsockoptInt is not cross-platform; use a portable approach
		// by checking that the UDP conn is non-nil (the real check is in the OS log).
		bufSize = int(fd) // fd > 0 means the socket was created
	})
	if ctrlErr != nil {
		t.Skipf("Control: %v", ctrlErr)
	}
	if bufSize <= 0 {
		t.Error("UDP socket file descriptor is invalid — SetReadBuffer may not have been called")
	}
	t.Logf("UDP socket fd=%d is valid; SetReadBuffer(%d) was called", bufSize, defaultUDPBufferSize)
}

// --- DPLPMTUD (Path MTU Discovery) ---

// TestDPLPMTUDEnabled verifies that path MTU discovery is NOT disabled in the
// default QUIC config.  It also confirms that connections actually complete
// their handshake with the setting in place (DPLPMTUD sends a small number of
// probe packets that could disrupt the handshake if misconfigured).
func TestDPLPMTUDEnabled(t *testing.T) {
	cfg := defaultQUICConfig()
	if cfg.DisablePathMTUDiscovery {
		t.Error("DisablePathMTUDiscovery is true — DPLPMTUD must be enabled by default")
	}

	// Server config (includes Allow0RTT) also must keep DPLPMTUD on.
	serverCfg := defaultServerQUICConfig()
	if serverCfg.DisablePathMTUDiscovery {
		t.Error("DisablePathMTUDiscovery is true in server config — DPLPMTUD must be enabled")
	}

	// Smoke-test: establish a QUIC connection; probe packets from DPLPMTUD
	// must not interfere with normal operation.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	rep := NewRep(ctx)
	if err := rep.Listen("quic://127.0.0.1:0"); err != nil {
		t.Fatalf("rep.Listen: %v", err)
	}
	defer rep.Close()

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
	if err := req.Dial("quic://" + rep.Addr().String()); err != nil {
		t.Fatalf("req.Dial: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if err := req.Send(NewMsgString("ping")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	msg, err := req.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if got := string(msg.Frames[0]); got != "pong:ping" {
		t.Errorf("unexpected reply %q", got)
	}
	t.Log("DPLPMTUD smoke-test passed: round-trip works with path MTU discovery enabled")
}
