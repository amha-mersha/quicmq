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

// Option name constants for SetOption / GetOption.
const (
	OptionSubscribe   = "SUBSCRIBE"
	OptionUnsubscribe = "UNSUBSCRIBE"
	OptionHWM         = "HWM"
)
