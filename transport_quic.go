package quicmq

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

// Default QUIC flow-control window sizes.
// These are generous defaults that avoid the "failed to sufficiently
// increase receive buffer size" warning on Linux.
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

// defaultQUICConfig returns a quic.Config with increased flow-control windows
// and short keep-alive / idle-timeout so dead peers are detected promptly.
func defaultQUICConfig() *quic.Config {
	return &quic.Config{
		InitialStreamReceiveWindow:     defaultInitialStreamWindow,
		MaxStreamReceiveWindow:         defaultMaxStreamWindow,
		InitialConnectionReceiveWindow: defaultInitialConnWindow,
		MaxConnectionReceiveWindow:     defaultMaxConnWindow,
		KeepAlivePeriod:                defaultKeepAlivePeriod,
		MaxIdleTimeout:                 defaultMaxIdleTimeout,
	}
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

	tr := &quic.Transport{Conn: udpConn}
	qcfg := defaultQUICConfig()
	qconn, err := tr.Dial(ctx, udpAddr, tlsCfg, qcfg)
	if err != nil {
		udpConn.Close()
		return nil, fmt.Errorf("quicmq: dial %q: %w", addr, err)
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

	tr := &quic.Transport{Conn: udpConn}
	qcfg := defaultQUICConfig()
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

// Verify net.Conn implementation at compile time.
var _ net.Conn = (*streamConn)(nil)

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
