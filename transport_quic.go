package quicmq

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
)

// Default QUIC flow-control window sizes.
const (
	defaultInitialStreamWindow = 8 << 20  // 8 MiB
	defaultMaxStreamWindow     = 16 << 20 // 16 MiB
	defaultInitialConnWindow   = 16 << 20 // 16 MiB
	defaultMaxConnWindow       = 32 << 20 // 32 MiB

	// Keep-alive / idle-timeout defaults.
	//
	// libzmq detects dead peers via ZMQ_HEARTBEAT_IVL (default disabled) or
	// the OS-level TCP keepalive. We enable QUIC's built-in keep-alive so a
	// disappearing peer (process crash, network partition, kill -9) is
	// detected within a few seconds instead of the 30-second default
	// MaxIdleTimeout in quic-go. Without this, subscribers/requesters will
	// freeze when the peer is ungracefully terminated.
	defaultKeepAlivePeriod = 5 * time.Second
	defaultMaxIdleTimeout  = 15 * time.Second

	// defaultUDPBufferSize is the target OS-level UDP socket send/receive
	// buffer size (7 MiB).  This matches the value recommended by quic-go
	// (https://quic-go.net/docs/quic/optimizations/) and what quic-go itself
	// requests internally.  The OS may grant less than this if the system
	// limit has not been raised (Linux: net.core.rmem_max / wmem_max).
	// Use WithUDPBufferSize to override for a specific deployment.
	defaultUDPBufferSize = 7 << 20 // 7 MiB
)

// quicTransport implements the Transport interface using QUIC.
type quicTransport struct{}

func init() {
	must := func(err error) {
		if err != nil {
			panic(fmt.Errorf("quicmq: %+v", err))
		}
	}
	must(RegisterTransport("quic", &quicTransport{}))
}

// defaultQUICConfig returns a quic.Config with increased flow-control windows,
// short keep-alive / idle-timeout, and qlog tracing wired in.
//
// Optimizations applied (https://quic-go.net/docs/quic/optimizations/):
//
//   - GSO (Generic Segmentation Offload): enabled automatically by quic-go
//     because we pass *net.UDPConn (implements OOBCapablePacketConn).  This
//     batches multiple UDP packets into one kernel call on Linux 4.18+.
//
//   - DPLPMTUD (Path MTU Discovery, RFC 8899): enabled by leaving
//     DisablePathMTUDiscovery at its default (false).  quic-go probes for
//     larger packet sizes, reducing per-packet overhead on >1200-byte MTU
//     paths.
//
//   - UDP buffer sizes: we explicitly call SetReadBuffer/SetWriteBuffer
//     (defaultUDPBufferSize = 7 MiB) before passing the conn to quic.Transport.
//     On systems where net.core.rmem_max has been raised the full 7 MiB is
//     granted; otherwise the OS cap applies.  Use WithUDPBufferSize to tune.
//
// qlog output is written when the QLOGDIR environment variable is set (one
// .sqlog file per connection).  Callers may override the Tracer field to write
// to a custom directory via makeQlogTracer.
func defaultQUICConfig() *quic.Config {
	return &quic.Config{
		InitialStreamReceiveWindow:     defaultInitialStreamWindow,
		MaxStreamReceiveWindow:         defaultMaxStreamWindow,
		InitialConnectionReceiveWindow: defaultInitialConnWindow,
		MaxConnectionReceiveWindow:     defaultMaxConnWindow,
		KeepAlivePeriod:                defaultKeepAlivePeriod,
		MaxIdleTimeout:                 defaultMaxIdleTimeout,
		// Explicitly keep DPLPMTUD enabled (the default).  quic-go sends
		// fewer than 10 probe packets per connection to discover path MTU.
		DisablePathMTUDiscovery: false,
		Tracer:                  qlog.DefaultConnectionTracer,
	}
}

// defaultServerQUICConfig returns the base config plus Allow0RTT so the
// server issues session tickets and accepts 0-RTT early data on reconnects.
func defaultServerQUICConfig() *quic.Config {
	cfg := defaultQUICConfig()
	cfg.Allow0RTT = true
	return cfg
}

// ctxKeyUDPBufferSize is the context key for the UDP socket buffer size.
type ctxKeyUDPBufferSize struct{}

func withUDPBufferSize(ctx context.Context, size int) context.Context {
	return context.WithValue(ctx, ctxKeyUDPBufferSize{}, size)
}

func udpBufferSizeFromContext(ctx context.Context) int {
	if v, ok := ctx.Value(ctxKeyUDPBufferSize{}).(int); ok && v > 0 {
		return v
	}
	return defaultUDPBufferSize
}

// setUDPBuffers requests large OS-level send and receive buffers on conn.
// quic-go also requests this internally; calling it here first ensures our
// intent is explicit and benefits from any sysctl changes already in place
// (Linux: net.core.rmem_max / wmem_max ≥ 7340032).
// Errors are intentionally ignored: if the OS grants less than requested,
// quic-go will log a warning with instructions to increase the system limit.
func setUDPBuffers(conn *net.UDPConn, size int) {
	_ = conn.SetReadBuffer(size)
	_ = conn.SetWriteBuffer(size)
}

// ctxKeyStatelessReset is the context key for the QUIC stateless reset key.
type ctxKeyStatelessReset struct{}

// withStatelessResetKey stores a stateless reset key in ctx.
// Both Dial and Listen read it from ctx and apply it to their quic.Transport,
// enabling stateless reset (RFC 9000 §10.3): when a peer receives a packet for
// an unknown connection (e.g. after a crash), it sends a reset token derived
// from this key + the connection ID, letting the remote side detect the dead
// connection within ~1 RTT instead of waiting for the idle timeout.
func withStatelessResetKey(ctx context.Context, key [32]byte) context.Context {
	k := quic.StatelessResetKey(key)
	return context.WithValue(ctx, ctxKeyStatelessReset{}, k)
}

// statelessResetKeyFromContext extracts the stateless reset key, or nil.
func statelessResetKeyFromContext(ctx context.Context) *quic.StatelessResetKey {
	v, ok := ctx.Value(ctxKeyStatelessReset{}).(quic.StatelessResetKey)
	if !ok {
		return nil
	}
	return &v
}

// Dial creates a new QUIC connection to addr, opens a bidirectional stream,
// and returns a streamConn wrapping it.
func (t *quicTransport) Dial(ctx context.Context, addr string) (net.Conn, error) {
	tlsCfg := clientTLSFromContext(ctx)
	if tlsCfg == nil {
		tlsCfg = InsecureClientTLSConfig()
	}

	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("quicmq: resolve addr %q: %w", addr, err)
	}

	// Bind to any available local port.
	udpConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		return nil, fmt.Errorf("quicmq: listen udp: %w", err)
	}
	setUDPBuffers(udpConn, udpBufferSizeFromContext(ctx))

	tr := &quic.Transport{
		Conn:              udpConn,
		StatelessResetKey: statelessResetKeyFromContext(ctx),
	}
	qcfg := defaultQUICConfig()
	if dir := qlogDirFromContext(ctx); dir != "" {
		qcfg.Tracer = makeQlogTracer(dir)
	}

	// DialEarly starts the TLS 1.3 handshake and enables 0-RTT session resumption
	// when a cached session ticket is available.  On a cold start it behaves
	// identically to Dial.
	qconn, err := tr.DialEarly(ctx, udpAddr, tlsCfg, qcfg)
	if err != nil {
		udpConn.Close()
		return nil, fmt.Errorf("quicmq: dial %q: %w", addr, err)
	}

	// Wait for the TLS handshake to complete before opening a stream.
	// Opening a stream on the EarlyConnection before this point would race
	// with quic-go's handleHandshakeComplete (which applies transport
	// parameters used by the stream's flow controller), and would produce
	// a 0-RTT-rejected stream when the server rejects early data (e.g. a
	// stale session ticket from a different server).  We still get the
	// 0-RTT latency benefit for the handshake itself; we just don't send
	// the ZMTP greeting as early data, which requires a server response anyway.
	select {
	case <-qconn.HandshakeComplete():
	case <-ctx.Done():
		qconn.CloseWithError(0, "context cancelled during handshake")
		udpConn.Close()
		return nil, fmt.Errorf("quicmq: handshake for %q: %w", addr, ctx.Err())
	}

	// NextConnection finalises the post-handshake state (calls UseResetMaps
	// internally).  Since we already waited on HandshakeComplete above, this
	// returns immediately and is safe to call on every code path.
	if _, err = qconn.NextConnection(ctx); err != nil {
		qconn.CloseWithError(0, "post-handshake NextConnection failed")
		udpConn.Close()
		return nil, fmt.Errorf("quicmq: post-handshake for %q: %w", addr, err)
	}

	stream, err := qconn.OpenStreamSync(ctx)
	if err != nil {
		qconn.CloseWithError(0, "stream open failed")
		udpConn.Close()
		return nil, fmt.Errorf("quicmq: open stream: %w", err)
	}

	return &streamConn{
		stream:     stream,
		qconn:      qconn,
		transport:  tr,
		localAddr:  udpConn.LocalAddr(),
		remoteAddr: qconn.RemoteAddr(),
	}, nil
}

// Listen creates a QUIC listener on the given addr. The returned net.Listener
// yields a streamConn for each incoming bidirectional stream.
func (t *quicTransport) Listen(ctx context.Context, addr string) (net.Listener, error) {
	tlsCfg := serverTLSFromContext(ctx)
	if tlsCfg == nil {
		tlsCfg = GenerateTLSConfig()
	}

	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("quicmq: resolve addr %q: %w", addr, err)
	}

	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, fmt.Errorf("quicmq: listen udp on %q: %w", addr, err)
	}
	setUDPBuffers(udpConn, udpBufferSizeFromContext(ctx))

	tr := &quic.Transport{
		Conn:              udpConn,
		StatelessResetKey: statelessResetKeyFromContext(ctx),
	}
	qcfg := defaultServerQUICConfig() // includes Allow0RTT
	if dir := qlogDirFromContext(ctx); dir != "" {
		qcfg.Tracer = makeQlogTracer(dir)
	}
	ql, err := tr.Listen(tlsCfg, qcfg)
	if err != nil {
		udpConn.Close()
		return nil, fmt.Errorf("quicmq: quic listen: %w", err)
	}

	l := &quicListener{
		ql:       ql,
		tr:       tr,
		udpConn:  udpConn,
		addr:     udpConn.LocalAddr(),
		acceptCh: make(chan net.Conn, 16),
		closeCh:  make(chan struct{}),
	}
	go l.acceptLoop(ctx)
	return l, nil
}

// parseQUICAddr strips the quic:// scheme prefix if present.
func parseQUICAddr(addr string) string {
	for _, prefix := range []string{"quic://", "quic+udp://", "udp://"} {
		if strings.HasPrefix(addr, prefix) {
			return addr[len(prefix):]
		}
	}
	return addr
}

// --- streamConn: wraps quic.Stream as net.Conn ---

// streamConn adapts a QUIC stream to the net.Conn interface so it can be
// used by the connection layer (Conn) without knowing about QUIC specifics.
type streamConn struct {
	stream     *quic.Stream
	qconn      *quic.Conn
	transport  *quic.Transport
	localAddr  net.Addr
	remoteAddr net.Addr
}

func (sc *streamConn) Read(b []byte) (int, error)         { return sc.stream.Read(b) }
func (sc *streamConn) Write(b []byte) (int, error)        { return sc.stream.Write(b) }
func (sc *streamConn) LocalAddr() net.Addr                { return sc.localAddr }
func (sc *streamConn) RemoteAddr() net.Addr               { return sc.remoteAddr }
func (sc *streamConn) SetDeadline(t time.Time) error      { return sc.stream.SetDeadline(t) }
func (sc *streamConn) SetReadDeadline(t time.Time) error  { return sc.stream.SetReadDeadline(t) }
func (sc *streamConn) SetWriteDeadline(t time.Time) error { return sc.stream.SetWriteDeadline(t) }

func (sc *streamConn) Close() error {
	// Close the stream; the QUIC connection stays open for other streams.
	return sc.stream.Close()
}

// isZMTPConn implements zmtpMarker so Open() performs the ZMTP 3.1 handshake
// and uses ZMTP framing on QUIC streams, matching the TCP transport exactly.
func (*streamConn) isZMTPConn() {}

// Verify net.Conn and zmtpMarker implementations at compile time.
var _ net.Conn    = (*streamConn)(nil)
var _ zmtpMarker  = (*streamConn)(nil)

// --- quicListener: net.Listener over QUIC ---

// quicListener implements net.Listener by accepting QUIC connections and
// yielding a streamConn for each incoming bidirectional stream.
type quicListener struct {
	ql       *quic.Listener
	tr       *quic.Transport
	udpConn  *net.UDPConn
	addr     net.Addr
	acceptCh chan net.Conn
	closeCh  chan struct{}
	once     sync.Once
}

func (l *quicListener) acceptLoop(ctx context.Context) {
	for {
		select {
		case <-l.closeCh:
			return
		default:
		}

		qconn, err := l.ql.Accept(ctx)
		if err != nil {
			select {
			case <-l.closeCh:
			default:
			}
			return
		}
		go l.handleConn(ctx, qconn)
	}
}

func (l *quicListener) handleConn(ctx context.Context, qconn *quic.Conn) {
	for {
		select {
		case <-l.closeCh:
			return
		default:
		}

		stream, err := qconn.AcceptStream(ctx)
		if err != nil {
			return
		}

		sc := &streamConn{
			stream:     stream,
			qconn:      qconn,
			localAddr:  l.addr,
			remoteAddr: qconn.RemoteAddr(),
		}

		select {
		case l.acceptCh <- sc:
		case <-l.closeCh:
			sc.Close()
			return
		}
	}
}

func (l *quicListener) Accept() (net.Conn, error) {
	select {
	case conn, ok := <-l.acceptCh:
		if !ok {
			return nil, fmt.Errorf("quicmq: listener closed")
		}
		return conn, nil
	case <-l.closeCh:
		return nil, fmt.Errorf("quicmq: listener closed")
	}
}

func (l *quicListener) Close() error {
	l.once.Do(func() {
		close(l.closeCh)
	})
	err := l.ql.Close()
	l.udpConn.Close()
	return err
}

func (l *quicListener) Addr() net.Addr {
	return l.addr
}

var _ net.Listener = (*quicListener)(nil)

func isErr0RTTRejected(err error) bool {
	return errors.Is(err, quic.Err0RTTRejected)
}
