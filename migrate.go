package quicmq

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/quic-go/quic-go"
)

// MigrateToNewPath migrates the first live QUIC connection on sock to a fresh
// local UDP port, implementing RFC 9000 §9 connection migration.
//
//  1. Binds a new local UDP socket on an ephemeral port.
//  2. Calls qconn.AddPath to register it as an additional QUIC path.
//  3. Calls path.Probe — sends PATH_CHALLENGE, waits for PATH_RESPONSE.
//  4. Calls path.Switch — makes the new path the primary path.
//
// Returns (oldLocalAddr, newLocalAddr, nil) on success.
// Errors if sock has no live QUIC connection (e.g. TCP transport or not yet dialled).
func MigrateToNewPath(ctx context.Context, sock Socket) (oldAddr, newAddr string, err error) {
	base := socketBase(sock)
	if base == nil {
		return "", "", fmt.Errorf("quicmq: MigrateToNewPath: unsupported socket type")
	}

	var qconn *quic.Conn
	base.mu.RLock()
	for _, c := range base.conns {
		if sc, ok := c.rw.(*streamConn); ok {
			qconn = sc.qconn
			break
		}
	}
	base.mu.RUnlock()

	if qconn == nil {
		return "", "", fmt.Errorf("quicmq: MigrateToNewPath: no live QUIC connection found (TCP transport?)")
	}

	oldAddr = qconn.LocalAddr().String()

	udpConn, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		return oldAddr, "", fmt.Errorf("quicmq: MigrateToNewPath: bind UDP: %w", err)
	}

	tr := &quic.Transport{Conn: udpConn}
	path, err := qconn.AddPath(tr)
	if err != nil {
		udpConn.Close()
		return oldAddr, "", fmt.Errorf("quicmq: MigrateToNewPath: AddPath: %w", err)
	}

	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := path.Probe(probeCtx); err != nil {
		path.Close()
		udpConn.Close()
		return oldAddr, "", fmt.Errorf("quicmq: MigrateToNewPath: Probe (PATH_CHALLENGE): %w", err)
	}

	if err := path.Switch(); err != nil {
		path.Close()
		udpConn.Close()
		return oldAddr, "", fmt.Errorf("quicmq: MigrateToNewPath: Switch: %w", err)
	}

	newAddr = udpConn.LocalAddr().String()
	return oldAddr, newAddr, nil
}

// QuicLocalAddr returns the local UDP address string of the first live QUIC
// connection on sock. Returns "" if the socket has no QUIC connection (e.g.
// TCP transport or not yet dialled).
func QuicLocalAddr(sock Socket) string {
	base := socketBase(sock)
	if base == nil {
		return ""
	}
	base.mu.RLock()
	defer base.mu.RUnlock()
	for _, c := range base.conns {
		if sc, ok := c.rw.(*streamConn); ok {
			return sc.qconn.LocalAddr().String()
		}
	}
	return ""
}

// socketBase extracts the embedded *socket from any known public socket type.
// Returns nil for unsupported types.
func socketBase(sock Socket) *socket {
	switch s := sock.(type) {
	case *reqSocket:
		return s.socket
	case *repSocket:
		return s.socket
	case *pubSocket:
		return s.socket
	case *subSocket:
		return s.socket
	default:
		return nil
	}
}
