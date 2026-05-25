package quicmq

import (
	"context"
	"fmt"
	"net"
)

// tcpTransport implements Transport over plain TCP using ZMTP 3.1 framing.
// Register under the "tcp" scheme so endpoints look like "tcp://host:port".
type tcpTransport struct{}

func init() {
	must := func(err error) {
		if err != nil {
			panic(fmt.Errorf("quicmq: %+v", err))
		}
	}
	must(RegisterTransport("tcp", &tcpTransport{}))
}

// ─── Context keys for CURVE key material ──────────────────────────────────────

type contextKeyCurveServer struct{}
type contextKeyCurveClientKey struct{}
type contextKeyCurveServerPK struct{}
type contextKeyCurveTimingDir struct{}

func withCurveServerKey(ctx context.Context, key CurveKey) context.Context {
	return context.WithValue(ctx, contextKeyCurveServer{}, key)
}

func curveServerKeyFromContext(ctx context.Context) (CurveKey, bool) {
	v, ok := ctx.Value(contextKeyCurveServer{}).(CurveKey)
	return v, ok
}

func withCurveClientKey(ctx context.Context, clientKey CurveKey, serverPK [32]byte) context.Context {
	ctx = context.WithValue(ctx, contextKeyCurveClientKey{}, clientKey)
	ctx = context.WithValue(ctx, contextKeyCurveServerPK{}, serverPK)
	return ctx
}

func curveClientKeyFromContext(ctx context.Context) (clientKey CurveKey, serverPK [32]byte, ok bool) {
	clientKey, ok = ctx.Value(contextKeyCurveClientKey{}).(CurveKey)
	if !ok {
		return
	}
	serverPK, _ = ctx.Value(contextKeyCurveServerPK{}).([32]byte)
	return
}

func withCurveTimingDir(ctx context.Context, dir string) context.Context {
	if dir == "" {
		return ctx
	}
	return context.WithValue(ctx, contextKeyCurveTimingDir{}, dir)
}

func curveTimingDirFromContext(ctx context.Context) string {
	dir, _ := ctx.Value(contextKeyCurveTimingDir{}).(string)
	return dir
}

// ─── TCP transport ─────────────────────────────────────────────────────────────

// Dial opens a TCP connection to addr.  When CURVE keys are present in ctx
// (set via WithCurveClient socket option), it returns a tcpCURVEConn so that
// Open() performs the CURVE handshake instead of NULL.
func (t *tcpTransport) Dial(ctx context.Context, addr string) (net.Conn, error) {
	d := &net.Dialer{}
	raw, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("quicmq: tcp dial %q: %w", addr, err)
	}
	if clientKey, serverPK, ok := curveClientKeyFromContext(ctx); ok {
		return &tcpCURVEConn{
			tcpRawConn:      tcpRawConn{Conn: raw},
			clientKey:       clientKey,
			serverPublicKey: serverPK,
			timingDir:       curveTimingDirFromContext(ctx),
		}, nil
	}
	return &tcpRawConn{Conn: raw}, nil
}

// Listen binds a TCP listener on addr.  When a server CURVE key is present in
// ctx (set via WithCurveServer socket option), the listener wraps each accepted
// connection in a tcpCURVEConn so Open() performs the CURVE handshake.
func (t *tcpTransport) Listen(ctx context.Context, addr string) (net.Listener, error) {
	lc := net.ListenConfig{}
	l, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("quicmq: tcp listen %q: %w", addr, err)
	}
	if serverKey, ok := curveServerKeyFromContext(ctx); ok {
		return &tcpCURVEListener{
			Listener:  l,
			serverKey: serverKey,
			timingDir: curveTimingDirFromContext(ctx),
		}, nil
	}
	return &tcpListener{Listener: l}, nil
}

// ─── NULL-mechanism TCP conn/listener ─────────────────────────────────────────

// tcpRawConn wraps a plain net.Conn and implements zmtpMarker so that Open()
// knows to perform the ZMTP 3.1 NULL handshake and use ZMTP wire framing.
type tcpRawConn struct{ net.Conn }

// isZMTPConn implements zmtpMarker — no logic needed, just the marker method.
func (*tcpRawConn) isZMTPConn() {}

// tcpListener wraps net.Listener and returns a tcpRawConn on every Accept.
type tcpListener struct{ net.Listener }

func (l *tcpListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &tcpRawConn{Conn: c}, nil
}

// ─── CURVE-mechanism TCP conn/listener ────────────────────────────────────────

// tcpCURVEConn embeds tcpRawConn (giving it zmtpMarker) and additionally
// implements curveTCPMarker so that Open() selects the CURVE handshake.
type tcpCURVEConn struct {
	tcpRawConn
	// Client side: own keypair + server's permanent public key.
	clientKey       CurveKey
	serverPublicKey [32]byte
	// Server side: own permanent keypair (populated by tcpCURVEListener).
	serverKey CurveKey
	// timingDir, when non-empty, causes the CURVE handshake to write per-step
	// timing JSON to this directory (analogous to QUIC qlog files).
	timingDir string
}

func (c *tcpCURVEConn) curveHandshakeTimingDir() string { return c.timingDir }

func (c *tcpCURVEConn) curveServerKey() CurveKey             { return c.serverKey }
func (c *tcpCURVEConn) curveClientKey() (CurveKey, [32]byte) { return c.clientKey, c.serverPublicKey }

// tcpCURVEListener stamps each accepted conn with the server's permanent keypair.
type tcpCURVEListener struct {
	net.Listener
	serverKey CurveKey
	timingDir string
}

func (l *tcpCURVEListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &tcpCURVEConn{
		tcpRawConn: tcpRawConn{Conn: c},
		serverKey:  l.serverKey,
		timingDir:  l.timingDir,
	}, nil
}

var _ Transport = (*tcpTransport)(nil)
