package quicmq

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/quic-go/quic-go"
)

// ConnectionPool multiplexes outgoing QUIC connections so that multiple
// sockets dialing the same remote endpoint share a single QUIC connection
// rather than each creating a separate UDP flow.
//
// This demonstrates one of QUIC's key advantages over TCP: the protocol's
// built-in stream multiplexing lets entirely different message-pattern sockets
// (e.g. PUB and REQ) ride the same underlying connection to a peer, avoiding
// the per-connection overhead (handshakes, flow-control windows, OS resources)
// that TCP would require.
//
// Usage:
//
//	pool := quicmq.NewConnectionPool()
//	pub := quicmq.NewPub(ctx, quicmq.WithConnectionPool(pool))
//	req := quicmq.NewReq(ctx, quicmq.WithConnectionPool(pool))
//	// pub and req will share one QUIC connection when dialing the same server.
type ConnectionPool struct {
	mu      sync.Mutex
	entries map[string]*poolEntry // key = remote addr (host:port)
}

type poolEntry struct {
	qconn   *quic.Conn
	tr      *quic.Transport
	udpConn *net.UDPConn
}

// NewConnectionPool creates an empty connection pool.
func NewConnectionPool() *ConnectionPool {
	return &ConnectionPool{
		entries: make(map[string]*poolEntry),
	}
}

// Dial returns a net.Conn backed by a new QUIC stream on an existing shared
// connection to addr. If no live connection exists, a new one is dialled and
// cached for subsequent callers.
//
// TLS and qlog settings are taken from ctx, just like the regular transport.
//
// This method satisfies the Transport interface so a pool can be passed
// directly to WithConnectionPool.
func (p *ConnectionPool) Dial(ctx context.Context, addr string) (net.Conn, error) {
	// Normalise the address (strip any scheme prefix).
	addr = parseQUICAddr(addr)

	tlsCfg := clientTLSFromContext(ctx)
	if tlsCfg == nil {
		tlsCfg = InsecureClientTLSConfig()
	}

	p.mu.Lock()
	entry, ok := p.entries[addr]
	if ok {
		// Verify the connection is still alive before handing out a stream.
		select {
		case <-entry.qconn.Context().Done():
			// Connection is dead — remove and fall through to dial a new one.
			entry.udpConn.Close()
			delete(p.entries, addr)
			ok = false
		default:
		}
	}

	if !ok {
		// Dial a fresh QUIC connection and cache it.
		udpAddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			p.mu.Unlock()
			return nil, fmt.Errorf("quicmq: pool resolve %q: %w", addr, err)
		}
		udpConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
		if err != nil {
			p.mu.Unlock()
			return nil, fmt.Errorf("quicmq: pool udp: %w", err)
		}
		tr := &quic.Transport{
				Conn:              udpConn,
				StatelessResetKey: statelessResetKeyFromContext(ctx),
			}
		qcfg := defaultQUICConfig()
		if dir := qlogDirFromContext(ctx); dir != "" {
			qcfg.Tracer = makeQlogTracer(dir)
		}
		qconn, err := tr.DialEarly(ctx, udpAddr, tlsCfg, qcfg)
		if err != nil {
			udpConn.Close()
			p.mu.Unlock()
			return nil, fmt.Errorf("quicmq: pool dial %q: %w", addr, err)
		}
		select {
		case <-qconn.HandshakeComplete():
		case <-ctx.Done():
			qconn.CloseWithError(0, "cancelled")
			udpConn.Close()
			p.mu.Unlock()
			return nil, ctx.Err()
		}
		entry = &poolEntry{qconn: qconn, tr: tr, udpConn: udpConn}
		p.entries[addr] = entry
	}
	p.mu.Unlock()

	// Open a new stream on the shared connection.
	stream, err := entry.qconn.OpenStreamSync(ctx)
	if err != nil {
		return nil, fmt.Errorf("quicmq: pool open stream: %w", err)
	}

	return &streamConn{
		stream:     stream,
		qconn:      entry.qconn,
		localAddr:  entry.udpConn.LocalAddr(),
		remoteAddr: entry.qconn.RemoteAddr(),
	}, nil
}

// Listen is not supported on a ConnectionPool — listening sockets always
// create their own QUIC listener.
func (p *ConnectionPool) Listen(_ context.Context, _ string) (net.Listener, error) {
	return nil, fmt.Errorf("quicmq: ConnectionPool does not support Listen")
}

// Close closes all cached QUIC connections and clears the pool.
func (p *ConnectionPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var errs []string
	for addr, e := range p.entries {
		if err := e.qconn.CloseWithError(0, "pool closed"); err != nil {
			errs = append(errs, err.Error())
		}
		e.udpConn.Close()
		delete(p.entries, addr)
	}
	if len(errs) > 0 {
		return fmt.Errorf("quicmq: pool close: %s", strings.Join(errs, "; "))
	}
	return nil
}

// Len returns the number of cached connections.
func (p *ConnectionPool) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.entries)
}

// LocalAddr returns the local UDP address of the cached connection to addr, or
// nil if no connection exists. Sockets sharing this pool use the same local
// address — useful in tests to verify that connection sharing is active.
func (p *ConnectionPool) LocalAddr(addr string) net.Addr {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.entries[addr]
	if !ok {
		return nil
	}
	return e.udpConn.LocalAddr()
}

var _ Transport = (*ConnectionPool)(nil)
