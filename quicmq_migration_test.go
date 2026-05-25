package quicmq

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
)

// TestConnectionMigration demonstrates QUIC RFC 9000 §9 connection migration:
// the client opens a connection on one local UDP port, then migrates to a
// second local port while keeping the same logical QUIC connection.
//
// After migration the server continues delivering data without interruption,
// proving that the connection persisted across the network-path change.
// In practice this lets a mobile client survive a Wi-Fi → 5G handover without
// re-establishing the connection or losing in-flight data.
func TestConnectionMigration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	trans := &quicTransport{}

	// ── Server ────────────────────────────────────────────────────────────────
	serverTLS := GenerateTLSConfig()
	serverCtx := withServerTLS(ctx, serverTLS)
	l, err := trans.Listen(serverCtx, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()
	serverAddr := l.Addr().String()
	t.Logf("server listening on %s", serverAddr)

	// Accept one connection and echo all data back.
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, err := l.Accept()
		if err != nil {
			return
		}
		io.Copy(conn, conn) //nolint:errcheck
	}()

	// ── Client: initial connection on path A ──────────────────────────────────
	clientTLS := InsecureClientTLSConfig()
	clientCtx := withClientTLS(ctx, clientTLS)

	conn1, err := trans.Dial(clientCtx, serverAddr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	sc := conn1.(*streamConn)
	defer conn1.Close()

	// Confirm connectivity: write and read back a small message.
	if _, err := conn1.Write([]byte("hello before migration\n")); err != nil {
		t.Fatalf("write before migration: %v", err)
	}
	buf := make([]byte, 64)
	n, err := conn1.Read(buf)
	if err != nil {
		t.Fatalf("read before migration: %v", err)
	}
	t.Logf("before migration: received %q", string(buf[:n]))

	localAddrBefore := conn1.LocalAddr().String()
	t.Logf("initial local address: %s", localAddrBefore)

	// ── Migration: add a new path on a different local port ───────────────────
	// Bind a second local UDP socket on a different ephemeral port.
	udpConn2, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("bind second udp socket: %v", err)
	}
	defer udpConn2.Close()

	newLocalAddr := udpConn2.LocalAddr().String()
	t.Logf("new local address (migration target): %s", newLocalAddr)

	// Wrap the second UDP socket in a quic.Transport.
	newTransport := &quic.Transport{Conn: udpConn2}

	// AddPath registers the new transport with the existing QUIC connection.
	path, err := sc.qconn.AddPath(newTransport)
	if err != nil {
		t.Fatalf("AddPath: %v", err)
	}
	defer path.Close()

	// Probe validates the new path: sends a PATH_CHALLENGE frame over the new
	// local port and waits for the server's PATH_RESPONSE (RFC 9000 §8.2).
	probeCtx, probeCancel := context.WithTimeout(ctx, 5*time.Second)
	defer probeCancel()

	t.Log("probing new path...")
	if err := path.Probe(probeCtx); err != nil {
		t.Fatalf("path.Probe: %v", err)
	}
	t.Log("path validated successfully")

	// Switch migrates the QUIC connection to the new path.  After this call all
	// QUIC packets flow through udpConn2.
	if err := path.Switch(); err != nil {
		t.Fatalf("path.Switch: %v", err)
	}
	t.Log("connection migrated to new path")

	// ── Verify: data still flows after migration ──────────────────────────────
	if _, err := conn1.Write([]byte("hello after migration\n")); err != nil {
		t.Fatalf("write after migration: %v", err)
	}
	n, err = conn1.Read(buf)
	if err != nil {
		t.Fatalf("read after migration: %v", err)
	}
	t.Logf("after migration: received %q", string(buf[:n]))
	t.Log("connection migration succeeded — data flows on new path")
}

// TestConnectionMigrationWithSocket demonstrates connection migration at the
// higher-level socket API: a REQ socket migrates its QUIC connection mid-session
// and then completes a request/reply round-trip on the new path.
//
// This mirrors a real-world scenario: a mobile client moves between networks
// while in the middle of a long-lived conversation with a server.
func TestConnectionMigrationWithSocket(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Start a REP server.
	rep := NewRep(ctx)
	defer rep.Close()
	if err := rep.Listen("quic://127.0.0.1:0"); err != nil {
		t.Fatalf("rep.Listen: %v", err)
	}
	endpoint := "quic://" + rep.Addr().String()
	t.Logf("REP listening on %s", endpoint)

	// Handle requests in the background.
	go func() {
		for {
			msg, err := rep.Recv()
			if err != nil {
				return
			}
			_ = rep.Send(NewMsgString("pong:" + string(msg.Frames[0])))
		}
	}()

	// Dial from the REQ side and confirm first round-trip.
	req := NewReq(ctx)
	defer req.Close()
	if err := req.Dial(endpoint); err != nil {
		t.Fatalf("req.Dial: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	if err := req.Send(NewMsgString("ping1")); err != nil {
		t.Fatalf("req.Send ping1: %v", err)
	}
	msg1, err := req.Recv()
	if err != nil {
		t.Fatalf("req.Recv ping1: %v", err)
	}
	t.Logf("before migration: %q", string(msg1.Frames[0]))

	// Reach into the socket to get the underlying QUIC connection.
	// req is a *reqSocket; its embedded *socket holds conn state.
	// We use the transport-level API directly for the migration.
	baseSock := req.(*reqSocket).socket

	var qconn *quic.Conn
	baseSock.mu.RLock()
	for _, c := range baseSock.conns {
		if sc, ok := c.rw.(*streamConn); ok {
			qconn = sc.qconn
			break
		}
	}
	baseSock.mu.RUnlock()

	if qconn == nil {
		t.Skip("could not access underlying quic.Conn — skipping migration sub-test")
	}

	// Bind a second local UDP socket and migrate.
	udpConn2, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("bind second udp: %v", err)
	}
	defer udpConn2.Close()

	newTransport := &quic.Transport{Conn: udpConn2}
	path, err := qconn.AddPath(newTransport)
	if err != nil {
		t.Fatalf("AddPath: %v", err)
	}
	defer path.Close()

	probeCtx, probeCancel := context.WithTimeout(ctx, 5*time.Second)
	defer probeCancel()
	if err := path.Probe(probeCtx); err != nil {
		t.Fatalf("path.Probe: %v", err)
	}
	if err := path.Switch(); err != nil {
		t.Fatalf("path.Switch: %v", err)
	}
	t.Logf("migrated to new path: %s", udpConn2.LocalAddr())

	// Confirm round-trip still works after migration.
	if err := req.Send(NewMsgString("ping2")); err != nil {
		t.Fatalf("req.Send ping2: %v", err)
	}
	msg2, err := req.Recv()
	if err != nil {
		t.Fatalf("req.Recv ping2: %v", err)
	}
	t.Logf("after migration: %q", string(msg2.Frames[0]))
	t.Log("socket-level migration succeeded")
}
