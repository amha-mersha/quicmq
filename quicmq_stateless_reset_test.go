package quicmq

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
)

// TestStatelessResetKeyOption verifies that WithStatelessResetKey stores the
// caller-supplied key in the socket and overrides the auto-generated default.
func TestStatelessResetKeyOption(t *testing.T) {
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	pub := NewPub(ctx, WithStatelessResetKey(key))
	defer pub.Close()

	baseSock := pub.(*pubSocket).socket
	if baseSock.statelessResetKey != key {
		t.Errorf("stateless reset key mismatch: got %x, want %x",
			baseSock.statelessResetKey, key)
	}
}

// TestStatelessResetDefaultKeyIsRandom verifies that two sockets created
// without WithStatelessResetKey each get a distinct auto-generated key.
func TestStatelessResetDefaultKeyIsRandom(t *testing.T) {
	ctx := context.Background()

	pub1 := NewPub(ctx)
	pub2 := NewPub(ctx)
	defer pub1.Close()
	defer pub2.Close()

	k1 := pub1.(*pubSocket).socket.statelessResetKey
	k2 := pub2.(*pubSocket).socket.statelessResetKey

	if k1 == k2 {
		t.Error("two sockets got identical stateless reset keys — random generation may be broken")
	}

	var zero [32]byte
	if k1 == zero || k2 == zero {
		t.Error("stateless reset key is all zeros — crypto/rand failed silently")
	}
}

// TestStatelessResetDetectsDeadServer verifies RFC 9000 §10.3 stateless reset:
// when a server crashes and restarts on the same port with the same key, the
// client detects the dead connection within ~1 RTT (far faster than the 15 s
// idle timeout that applies without stateless reset).
//
// Test sequence:
//  1. Start Server 1 with a known reset key.
//  2. Client connects and exchanges one message.
//  3. Crash Server 1 by closing its UDP socket (simulates kill -9; no
//     QUIC CONNECTION_CLOSE is sent, so the client doesn't know yet).
//  4. Start Server 2 on the same port with the same reset key.
//  5. Client writes to the dead stream — packets reach Server 2, which
//     doesn't recognise the connection ID and replies with a stateless
//     reset token derived from (key, connID).  The client matches the
//     token, closes the connection, and the pending Read unblocks with
//     an error.
//  6. Assert detection happened in < 5 s (idle timeout is 15 s).
func TestStatelessResetDetectsDeadServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Stable reset key reused by both server instances.
	var resetKey [32]byte
	if _, err := rand.Read(resetKey[:]); err != nil {
		t.Fatal(err)
	}

	trans := &quicTransport{}

	// ── Server 1 ──────────────────────────────────────────────────────────────
	serverTLS := GenerateTLSConfig()
	s1Ctx := withServerTLS(ctx, serverTLS)
	s1Ctx = withStatelessResetKey(s1Ctx, resetKey)

	l, err := trans.Listen(s1Ctx, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("server1 listen: %v", err)
	}
	serverPort := l.Addr().(*net.UDPAddr).Port
	ql1 := l.(*quicListener)
	t.Logf("server1 listening on port %d", serverPort)

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go io.Copy(io.Discard, conn) //nolint:errcheck
		}
	}()

	// ── Client ────────────────────────────────────────────────────────────────
	clientTLS := InsecureClientTLSConfig()
	clientCtx := withClientTLS(ctx, clientTLS)

	conn, err := trans.Dial(clientCtx, fmt.Sprintf("127.0.0.1:%d", serverPort))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	// Confirm connectivity so the handshake and NEW_CONNECTION_ID frames
	// (carrying stateless reset tokens) have been exchanged.
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatalf("pre-crash write: %v", err)
	}
	time.Sleep(150 * time.Millisecond) // let NEW_CONNECTION_ID frames arrive

	t.Logf("client connected; local=%s remote=%s", conn.LocalAddr(), conn.RemoteAddr())

	// ── Crash server 1 ────────────────────────────────────────────────────────
	// Close the UDP socket directly — no QUIC CONNECTION_CLOSE is sent.
	// This simulates kill -9 / power failure.
	t.Log("crashing server1 (UDP socket closed, no CONNECTION_CLOSE)...")
	ql1.udpConn.Close()
	time.Sleep(80 * time.Millisecond) // let acceptLoop exit

	// ── Server 2: restart on same port with same reset key ────────────────────
	s2Addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: serverPort}
	udpConn2, err := net.ListenUDP("udp", s2Addr)
	if err != nil {
		t.Fatalf("rebind port %d: %v", serverPort, err)
	}
	defer udpConn2.Close()

	k := quic.StatelessResetKey(resetKey)
	tr2 := &quic.Transport{
		Conn:              udpConn2,
		StatelessResetKey: &k,
	}
	defer tr2.Close()

	ql2, err := tr2.Listen(GenerateTLSConfig(), defaultServerQUICConfig())
	if err != nil {
		t.Fatalf("server2 listen: %v", err)
	}
	defer ql2.Close()

	go func() {
		for {
			qconn, err := ql2.Accept(ctx)
			if err != nil {
				return
			}
			go func(c *quic.Conn) {
				s, err := c.AcceptStream(ctx)
				if err != nil {
					return
				}
				io.Copy(io.Discard, s) //nolint:errcheck
			}(qconn)
		}
	}()
	t.Logf("server2 started on port %d with same reset key", serverPort)

	// ── Detection ─────────────────────────────────────────────────────────────
	start := time.Now()
	resetDetected := make(chan time.Duration, 1)

	// Read goroutine: blocks until the connection is reset by server 2.
	go func() {
		buf := make([]byte, 64)
		for {
			_, err := conn.Read(buf)
			if err != nil {
				select {
				case resetDetected <- time.Since(start):
				default:
				}
				return
			}
		}
	}()

	// Write goroutine: pokes the dead connection to elicit the stateless reset.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-resetDetected:
				return
			default:
				conn.Write([]byte("probe after crash")) //nolint:errcheck
				time.Sleep(20 * time.Millisecond)
			}
		}
	}()

	const maxWait = 5 * time.Second
	select {
	case elapsed := <-resetDetected:
		t.Logf("dead connection detected via stateless reset in %v (idle timeout = %v)",
			elapsed, defaultMaxIdleTimeout)
		if elapsed > maxWait {
			t.Errorf("detection too slow: %v > %v", elapsed, maxWait)
		}
	case <-time.After(maxWait):
		t.Errorf("stateless reset not detected within %v — key may not have been applied or packets not routed to server2", maxWait)
	}
}

// TestStatelessResetWithSocket verifies that the higher-level Socket API
// correctly propagates WithStatelessResetKey to the underlying quic.Transport.
//
// The test uses the Socket API for the server (REP) and a raw quicTransport
// client.  This combination proves that the key set via WithStatelessResetKey
// reaches the quic.Transport inside the socket's Listen path: when the server
// restarts with the same key, the raw client receives a stateless reset from
// the restarted server and detects the dead connection in sub-second time.
//
// Note: a pure socket-to-socket REQ/REP test is not ideal here because the
// REQ socket blocks on Recv after Send, letting the QUIC connection go quiet
// so no packets reach the restarted server to elicit a stateless reset.  Using
// a raw-transport client keeps the active write loop simple.
func TestStatelessResetWithSocket(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var resetKey [32]byte
	if _, err := rand.Read(resetKey[:]); err != nil {
		t.Fatal(err)
	}

	// ── Server 1: REP socket with stable reset key ────────────────────────────
	rep := NewRep(ctx, WithStatelessResetKey(resetKey))
	if err := rep.Listen("quic://127.0.0.1:0"); err != nil {
		t.Fatalf("rep.Listen: %v", err)
	}
	repPort := rep.Addr().(*net.UDPAddr).Port
	endpoint := fmt.Sprintf("quic://127.0.0.1:%d", repPort)
	t.Logf("REP server1 on port %d", repPort)

	go func() {
		for {
			msg, err := rep.Recv()
			if err != nil {
				return
			}
			_ = rep.Send(NewMsgString("pong:" + string(msg.Frames[0])))
		}
	}()

	// ── Client: raw quicTransport so we can keep an active write loop ─────────
	trans := &quicTransport{}
	clientCtx := withClientTLS(ctx, InsecureClientTLSConfig())

	conn, err := trans.Dial(clientCtx, fmt.Sprintf("127.0.0.1:%d", repPort))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	// Brief handshake + NEW_CONNECTION_ID exchange.
	if _, err := conn.Write([]byte("probe")); err != nil {
		t.Fatalf("initial write: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	t.Log("client connected; NEW_CONNECTION_ID frames exchanged")

	// ── Crash server 1 ────────────────────────────────────────────────────────
	repSock := rep.(*repSocket).socket
	repSock.listener.(*quicListener).udpConn.Close()
	t.Log("crashed REP server1 (UDP socket closed, no CONNECTION_CLOSE)")
	time.Sleep(80 * time.Millisecond)

	// ── Server 2: another REP socket with same reset key ──────────────────────
	rep2 := NewRep(ctx, WithStatelessResetKey(resetKey))
	if err := rep2.Listen(endpoint); err != nil {
		t.Fatalf("rep2.Listen: %v", err)
	}
	defer rep2.Close()
	go func() {
		for {
			msg, err := rep2.Recv()
			if err != nil {
				return
			}
			_ = rep2.Send(NewMsgString("pong2:" + string(msg.Frames[0])))
		}
	}()
	t.Logf("REP server2 restarted on %s with same reset key", endpoint)

	// ── Detection ─────────────────────────────────────────────────────────────
	// Active writes reach server2, which doesn't know this connection ID and
	// replies with a stateless reset derived from (resetKey, connID).
	// The client matches the token (stored from server1's NEW_CONNECTION_ID) and
	// closes the connection — Read returns an error well within maxWait.
	start := time.Now()
	resetDetected := make(chan time.Duration, 1)

	go func() {
		buf := make([]byte, 64)
		for {
			_, err := conn.Read(buf)
			if err != nil {
				select {
				case resetDetected <- time.Since(start):
				default:
				}
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-resetDetected:
				return
			default:
				conn.Write([]byte("probe after crash")) //nolint:errcheck
				time.Sleep(20 * time.Millisecond)
			}
		}
	}()

	const maxWait = 5 * time.Second
	select {
	case elapsed := <-resetDetected:
		t.Logf("socket-API server: stateless reset detected in %v (idle timeout = %v)",
			elapsed, defaultMaxIdleTimeout)
		if elapsed > maxWait {
			t.Errorf("detection too slow: %v > %v — socket API may not be propagating the key", elapsed, maxWait)
		}
	case <-time.After(maxWait):
		t.Errorf("stateless reset not detected within %v — WithStatelessResetKey may not have reached quic.Transport", maxWait)
	}
}
