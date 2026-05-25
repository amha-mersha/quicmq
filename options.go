package quicmq

import (
	"crypto/tls"
	"log"
	"time"
)

// Option configures some aspect of a QuicMQ socket.
type Option func(s *socket)

// WithTimeout sets the timeout value for socket send/recv operations.
func WithTimeout(timeout time.Duration) Option {
	return func(s *socket) {
		s.timeout = timeout
	}
}

// WithDialerRetry configures the time to wait between failed dial attempts.
func WithDialerRetry(retry time.Duration) Option {
	return func(s *socket) {
		s.retry = retry
	}
}

// WithDialerMaxRetries configures the maximum number of retries
// when dialing an endpoint (-1 means infinite retries).
func WithDialerMaxRetries(maxRetries int) Option {
	return func(s *socket) {
		s.maxRetries = maxRetries
	}
}

// WithDialTimeout sets an overall wall-clock budget for Dial (libzmq's
// ZMQ_CONNECT_TIMEOUT semantics). When non-zero, Dial gives up after
// this duration regardless of the retry/maxRetries settings.
//
// This is the right knob when you want "subscriber waits at most N
// seconds for the publisher to come up, then exits". Using only
// WithDialerRetry + WithDialerMaxRetries is unreliable because each
// failed QUIC handshake attempt can itself take several seconds.
//
// Zero (the default) disables the wall-clock timeout — retries are
// bounded only by maxRetries.
func WithDialTimeout(timeout time.Duration) Option {
	return func(s *socket) {
		s.dialTimeout = timeout
	}
}

// WithLogger sets a dedicated log.Logger for the socket.
func WithLogger(msg *log.Logger) Option {
	return func(s *socket) {
		s.log = msg
	}
}

// WithListenTLS sets the TLS configuration used when listening (server-side).
// For development, use GenerateTLSConfig. For production, use NewTLSConfig.
func WithListenTLS(cfg *tls.Config) Option {
	return func(s *socket) {
		s.tlsCfg = cfg
	}
}

// WithDialTLS sets the TLS configuration used when dialing (client-side).
// For development, use InsecureClientTLSConfig. For production, use
// NewClientTLSConfig.
func WithDialTLS(cfg *tls.Config) Option {
	return func(s *socket) {
		s.clientTlsCfg = cfg
	}
}

// WithAutomaticReconnect enables or disables automatic reconnection when a
// dialed connection is lost. When enabled, the socket will attempt to re-dial
// the endpoint using exponential backoff with jitter, matching libzmq's
// reconnect_ivl behavior.
//
// Defaults:
//
//	reconnect_ivl     = 100ms  (initial/base interval)
//	reconnect_ivl_max = 0      (disabled; uses fixed interval + jitter)
//
// When reconnect_ivl_max > 0, exponential backoff is used: the interval
// doubles on each attempt, capped at reconnect_ivl_max.
//
// When reconnect_ivl_max == 0 (default), a fixed interval with random jitter
// is used: reconnect_ivl + random(0, reconnect_ivl).
func WithAutomaticReconnect(automaticReconnect bool) Option {
	return func(s *socket) {
		s.autoReconnect = automaticReconnect
	}
}

// WithReconnectInterval sets the base reconnection interval (libzmq's
// ZMQ_RECONNECT_IVL). Default is 100ms.
func WithReconnectInterval(ivl time.Duration) Option {
	return func(s *socket) {
		s.reconnectIvl = ivl
	}
}

// WithReconnectIntervalMax sets the maximum reconnection interval for
// exponential backoff (libzmq's ZMQ_RECONNECT_IVL_MAX). Default is 0
// (disabled — fixed interval with jitter is used instead).
func WithReconnectIntervalMax(ivlMax time.Duration) Option {
	return func(s *socket) {
		s.reconnectIvlMax = ivlMax
	}
}

// WithConnectionPool sets a ConnectionPool for outgoing dial calls.
//
// All sockets sharing the same pool will reuse existing QUIC connections when
// dialing the same remote address.  This demonstrates QUIC's stream-level
// multiplexing: multiple messaging patterns (PUB, REQ, …) can share one UDP
// flow, avoiding redundant handshakes and reducing per-socket overhead.
//
// The pool does not affect Listen calls — listening sockets always create their
// own QUIC listener.
func WithConnectionPool(pool *ConnectionPool) Option {
	return func(s *socket) {
		s.dialTransport = pool
	}
}

// WithQlogDir enables per-connection qlog tracing (RFC 9001 §A / IETF qlog
// draft) and writes one .sqlog file per QUIC connection into dir.
//
// Files are named <odcid>_client.sqlog and <odcid>_server.sqlog, where odcid
// is the original destination connection ID.  The directory is created
// automatically if it does not exist.
//
// When this option is not set, qlog output is still produced if the QLOGDIR
// environment variable points to a writable directory.
func WithQlogDir(dir string) Option {
	return func(s *socket) {
		s.qlogDir = dir
	}
}

// WithCurveServer enables ZMTP CURVE encryption for TCP Listen calls.
// The socket will use key as its permanent keypair during the CURVE handshake.
// Clients must be configured with WithCurveClient and key.Public as the server
// public key.  Has no effect on QUIC connections (which use TLS 1.3 natively).
func WithCurveServer(key CurveKey) Option {
	return func(s *socket) {
		s.curveServerKey = &key
	}
}

// WithCurveClient enables ZMTP CURVE encryption for TCP Dial calls.
// clientKey is the client's permanent keypair; serverPublicKey is the server's
// permanent public key (obtained out-of-band, e.g. embedded in config).
// Has no effect on QUIC connections.
func WithCurveClient(clientKey CurveKey, serverPublicKey [32]byte) Option {
	return func(s *socket) {
		s.curveClientKey = &clientKey
		s.curveServerPublicKey = &serverPublicKey
	}
}

// WithCurveTimingDir enables per-handshake CURVE timing instrumentation.
// When set, each CURVE handshake writes a JSON timing file into dir recording
// elapsed milliseconds at each step (HELLO, WELCOME, INITIATE, READY).
// These files are analogous to QUIC qlog .sqlog files and can be processed
// by the handshake-analyze tool for a side-by-side post-handshake comparison.
// Has no effect on QUIC connections (use WithQlogDir for those).
func WithCurveTimingDir(dir string) Option {
	return func(s *socket) {
		s.curveTimingDir = dir
	}
}

// WithUDPBufferSize sets the target OS-level send and receive buffer size for
// the UDP socket underlying each QUIC connection created by this socket.
//
// The default is 7 MiB, matching the quic-go recommendation
// (https://quic-go.net/docs/quic/optimizations/).  The OS may grant less than
// the requested size unless the system limit has been raised:
//
//	# Linux — raise the kernel maximum (persists only until reboot):
//	sysctl -w net.core.rmem_max=7340032
//	sysctl -w net.core.wmem_max=7340032
//
// This does not affect QUIC flow-control windows (see the default 8–32 MiB
// values in transport_quic.go); it affects only the kernel UDP ring buffer
// that sits below QUIC.  Larger buffers reduce packet loss under bursty load.
func WithUDPBufferSize(size int) Option {
	return func(s *socket) {
		s.udpBufferSize = size
	}
}

// WithStatelessResetKey sets a persistent QUIC stateless reset key (RFC 9000 §10.3).
//
// By default quicmq generates a random key per socket at startup, which enables
// stateless reset within a single process lifetime. Set a stable, secret [32]byte
// key that you persist across restarts to also enable cross-restart fast detection:
// after a server crash and reboot with the same key, peers will receive a stateless
// reset token and detect the dead connection within ~1 RTT rather than waiting for
// the idle timeout.
//
// The key must be kept secret; treat it like a symmetric cryptographic secret.
func WithStatelessResetKey(key [32]byte) Option {
	return func(s *socket) {
		s.statelessResetKey = key
	}
}

// Option name constants for SetOption / GetOption.
const (
	OptionSubscribe   = "SUBSCRIBE"
	OptionUnsubscribe = "UNSUBSCRIBE"
	OptionHWM         = "HWM"
)
