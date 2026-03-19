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

// WithServerTLS sets the TLS configuration used when listening (server-side).
func WithServerTLS(cfg *tls.Config) Option {
	return func(s *socket) {
		s.tlsCfg = cfg
	}
}

// WithClientTLS sets the TLS configuration used when dialing (client-side).
func WithClientTLS(cfg *tls.Config) Option {
	return func(s *socket) {
		s.clientTlsCfg = cfg
	}
}

// Option name constants for SetOption / GetOption.
const (
	OptionSubscribe   = "SUBSCRIBE"
	OptionUnsubscribe = "UNSUBSCRIBE"
	OptionHWM         = "HWM"
)
