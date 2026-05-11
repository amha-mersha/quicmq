package quicmq

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"
)

const (
	defaultRetry      = 250 * time.Millisecond
	defaultTimeout    = 5 * time.Minute
	defaultMaxRetries = 10

	// libzmq defaults for reconnection.
	defaultReconnectIvl    = 100 * time.Millisecond // ZMQ_RECONNECT_IVL default
	defaultReconnectIvlMax = 0                      // ZMQ_RECONNECT_IVL_MAX default (disabled)
)

var (
	errInvalidAddress = errors.New("quicmq: invalid address")
	ErrBadProperty    = errors.New("quicmq: bad property")
)

// socket is the base implementation shared by all socket types.
type socket struct {
	ep          string
	typ         SocketType
	retry       time.Duration
	maxRetries  int
	dialTimeout time.Duration // total wall-clock budget for Dial (0 = unbounded)
	log         *log.Logger
	onConnAdded func(c *Conn) // optional callback invoked when a new connection is added
	timeout     time.Duration

	// TLS configs (QUIC-specific, but kept here for convenience).
	tlsCfg       *tls.Config
	clientTlsCfg *tls.Config

	// Automatic reconnection (matching libzmq's reconnect_ivl behavior).
	autoReconnect   bool
	reconnectIvl    time.Duration // base interval (default 100ms)
	reconnectIvlMax time.Duration // max interval for exp backoff (0=disabled)

	// isDialer tracks whether this socket connected via Dial (true) or Listen (false).
	// Reconnection only applies to dialed connections.
	isDialer bool

	mu    sync.RWMutex
	conns []*Conn
	r     rpool
	w     wpool

	props map[string]any

	ctx      context.Context
	cancel   context.CancelFunc
	listener net.Listener

	closedConns   []*Conn
	reaperCond    *sync.Cond
	reaperStarted bool
}

func newDefaultSocket(ctx context.Context, sockType SocketType) *socket {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	return &socket{
		typ:             sockType,
		retry:           defaultRetry,
		maxRetries:      defaultMaxRetries,
		timeout:         defaultTimeout,
		autoReconnect:   true,
		reconnectIvl:    defaultReconnectIvl,
		reconnectIvlMax: defaultReconnectIvlMax,
		conns:           nil,
		r:               newQReader(ctx),
		w:               newMWriter(ctx),
		props:           make(map[string]any),
		ctx:             ctx,
		cancel:          cancel,
		reaperCond:      sync.NewCond(&sync.Mutex{}),
	}
}

func newSocket(ctx context.Context, sockType SocketType, opts ...Option) *socket {
	sck := newDefaultSocket(ctx, sockType)
	for _, opt := range opts {
		opt(sck)
	}
	if sck.log == nil {
		sck.log = log.New(os.Stderr, "quicmq: ", 0)
	}
	return sck
}

func (sck *socket) topics() []string {
	var (
		keys   = make(map[string]struct{})
		topics []string
	)
	sck.mu.RLock()
	for _, con := range sck.conns {
		con.mu.RLock()
		for topic := range con.topics {
			if _, dup := keys[topic]; dup {
				continue
			}
			keys[topic] = struct{}{}
			topics = append(topics, topic)
		}
		con.mu.RUnlock()
	}
	sck.mu.RUnlock()
	return topics
}

// Close closes the socket.
func (sck *socket) Close() error {
	sck.reaperCond.L.Lock()
	sck.cancel()
	sck.reaperCond.Signal()
	sck.reaperCond.L.Unlock()

	if sck.listener != nil {
		defer sck.listener.Close()
	}

	sck.mu.RLock()
	defer sck.mu.RUnlock()

	var err error
	for _, conn := range sck.conns {
		e := conn.Close()
		if e != nil && err == nil {
			err = e
		}
	}
	return err
}

// Send puts the message on the outbound send queue.
func (sck *socket) Send(msg Msg) error {
	ctx, cancel := context.WithTimeout(sck.ctx, sck.Timeout())
	defer cancel()
	return sck.w.write(ctx, msg)
}

// SendMulti puts the message on the outbound send queue as multipart.
func (sck *socket) SendMulti(msg Msg) error {
	msg.multipart = true
	ctx, cancel := context.WithTimeout(sck.ctx, sck.Timeout())
	defer cancel()
	return sck.w.write(ctx, msg)
}

// Recv receives a complete message.
func (sck *socket) Recv() (Msg, error) {
	ctx, cancel := context.WithCancel(sck.ctx)
	defer cancel()
	var msg Msg
	err := sck.r.read(ctx, &msg)
	return msg, err
}

// Listen binds a local endpoint to the socket.
func (sck *socket) Listen(endpoint string) error {
	sck.ep = endpoint
	sck.isDialer = false
	network, addr, err := splitAddr(endpoint)
	if err != nil {
		return err
	}

	trans, ok := drivers.get(network)
	if !ok {
		return UnknownTransportError{Name: network}
	}

	// Embed server-side TLS config in context for the transport.
	listenCtx := withServerTLS(sck.ctx, sck.tlsCfg)

	l, err := trans.Listen(listenCtx, addr)
	if err != nil {
		return fmt.Errorf("quicmq: could not listen to %q: %w", endpoint, err)
	}
	sck.listener = l

	go sck.accept()
	if !sck.reaperStarted {
		sck.reaperCond.L.Lock()
		go sck.connReaper()
		sck.reaperStarted = true
	}

	return nil
}

func (sck *socket) accept() {
	ctx, cancel := context.WithCancel(sck.ctx)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			conn, err := sck.listener.Accept()
			if err != nil {
				continue
			}

			zconn, err := Open(conn, sck.typ, true, sck.scheduleRmConn)
			if err != nil {
				sck.log.Printf("could not open connection: %+v", err)
				continue
			}

			sck.addConn(zconn)
		}
	}
}

// Dial connects a remote endpoint to the socket.
//
// Retry behaviour matches libzmq:
//
//   - retry / maxRetries control per-attempt spacing and attempt count
//     (analogous to ZMQ_RECONNECT_IVL and an attempt cap).
//   - dialTimeout, when > 0, imposes a wall-clock budget on the entire
//     Dial call (analogous to ZMQ_CONNECT_TIMEOUT). The QUIC handshake
//     itself can take several seconds per attempt, so this is the
//     right knob for "give up after N seconds" semantics.
func (sck *socket) Dial(endpoint string) error {
	sck.ep = endpoint
	sck.isDialer = true
	network, addr, err := splitAddr(endpoint)
	if err != nil {
		return err
	}

	trans, ok := drivers.get(network)
	if !ok {
		return UnknownTransportError{Name: network}
	}

	// Embed client-side TLS config in context for the transport.
	dialCtx := withClientTLS(sck.ctx, sck.clientTlsCfg)

	if sck.dialTimeout > 0 {
		var cancel context.CancelFunc
		dialCtx, cancel = context.WithTimeout(dialCtx, sck.dialTimeout)
		defer cancel()
	}

	var (
		conn    net.Conn
		retries = 0
	)

connect:
	conn, err = trans.Dial(dialCtx, addr)
	if err != nil {
		// Wall-clock budget exhausted — surface the timeout immediately.
		if dialCtx.Err() != nil {
			return fmt.Errorf("quicmq: could not dial to %q within %s: %w", endpoint, sck.dialTimeout, err)
		}
		if (sck.maxRetries == -1 || retries < sck.maxRetries) && sck.ctx.Err() == nil {
			retries++
			select {
			case <-time.After(sck.retry):
			case <-dialCtx.Done():
				return fmt.Errorf("quicmq: could not dial to %q within %s: %w", endpoint, sck.dialTimeout, err)
			case <-sck.ctx.Done():
				return fmt.Errorf("quicmq: dial cancelled for %q: %w", endpoint, sck.ctx.Err())
			}
			goto connect
		}
		return fmt.Errorf("quicmq: could not dial to %q (retry=%v): %w", endpoint, sck.retry, err)
	}

	if conn == nil {
		return fmt.Errorf("quicmq: got a nil connection to %q", endpoint)
	}

	zconn, err := Open(conn, sck.typ, false, sck.scheduleRmConn)
	if err != nil {
		return fmt.Errorf("quicmq: could not open connection: %w", err)
	}

	if !sck.reaperStarted {
		sck.reaperCond.L.Lock()
		go sck.connReaper()
		sck.reaperStarted = true
	}
	sck.addConn(zconn)
	return nil
}

func (sck *socket) addConn(c *Conn) {
	sck.mu.Lock()
	defer sck.mu.Unlock()
	sck.conns = append(sck.conns, c)
	if sck.w != nil {
		sck.w.addConn(c)
	}
	if sck.r != nil {
		sck.r.addConn(c)
	}
	if sck.onConnAdded != nil {
		sck.onConnAdded(c)
	}
}

func (sck *socket) rmConn(c *Conn) {
	sck.mu.Lock()
	defer sck.mu.Unlock()

	cur := -1
	for i := range sck.conns {
		if sck.conns[i] == c {
			cur = i
			break
		}
	}
	if cur == -1 {
		return
	}

	sck.conns = append(sck.conns[:cur], sck.conns[cur+1:]...)
	if sck.r != nil {
		sck.r.rmConn(c)
	}
	if sck.w != nil {
		sck.w.rmConn(c)
	}
}

func (sck *socket) scheduleRmConn(c *Conn) {
	sck.reaperCond.L.Lock()
	sck.closedConns = append(sck.closedConns, c)
	sck.reaperCond.Signal()
	sck.reaperCond.L.Unlock()

	if sck.autoReconnect && sck.isDialer {
		go sck.reconnect()
	}
}

// reconnect implements libzmq-style reconnection with exponential backoff
// and jitter. See stream_connecter_base.cpp::get_new_reconnect_ivl().
//
// When reconnect_ivl_max > 0:
//
//	Exponential backoff: interval doubles each attempt, capped at ivl_max.
//
// When reconnect_ivl_max == 0 (default):
//
//	Fixed interval + random jitter: reconnect_ivl + random(0, reconnect_ivl).
func (sck *socket) reconnect() {
	currentIvl := time.Duration(-1)

	for sck.ctx.Err() == nil {
		interval := sck.getNewReconnectIvl(&currentIvl)

		select {
		case <-sck.ctx.Done():
			return
		case <-time.After(interval):
		}

		if sck.ctx.Err() != nil {
			return
		}

		err := sck.Dial(sck.ep)
		if err == nil {
			sck.log.Printf("reconnected to %s", sck.ep)
			return
		}
		sck.log.Printf("reconnect to %s failed: %v", sck.ep, err)
	}
}

// getNewReconnectIvl computes the next reconnection interval matching
// libzmq's stream_connecter_base_t::get_new_reconnect_ivl().
func (sck *socket) getNewReconnectIvl(currentIvl *time.Duration) time.Duration {
	if sck.reconnectIvlMax > 0 {
		// Exponential backoff capped at reconnect_ivl_max.
		var candidate time.Duration
		if *currentIvl < 0 {
			candidate = sck.reconnectIvl
		} else {
			candidate = *currentIvl * 2
			if candidate < *currentIvl {
				// overflow protection
				candidate = sck.reconnectIvlMax
			}
		}
		*currentIvl = min(candidate, sck.reconnectIvlMax)
		return *currentIvl
	}

	// Fixed interval + random jitter.
	if *currentIvl < 0 {
		*currentIvl = sck.reconnectIvl
	}
	jitter := time.Duration(rand.Int63n(int64(sck.reconnectIvl)))
	return *currentIvl + jitter
}

// Type returns the type of this socket.
func (sck *socket) Type() SocketType {
	return sck.typ
}

// Addr returns the listener's address, or nil if not listening.
func (sck *socket) Addr() net.Addr {
	if sck.listener == nil {
		return nil
	}
	return sck.listener.Addr()
}

// GetOption retrieves an option for a socket.
func (sck *socket) GetOption(name string) (any, error) {
	v, ok := sck.props[name]
	if !ok {
		return nil, ErrBadProperty
	}
	return v, nil
}

// SetOption sets an option for a socket.
func (sck *socket) SetOption(name string, value any) error {
	sck.props[name] = value
	return nil
}

// Timeout returns the socket's timeout duration.
func (sck *socket) Timeout() time.Duration {
	return sck.timeout
}

func (sck *socket) connReaper() {
	defer sck.reaperCond.L.Unlock()

	for {
		for len(sck.closedConns) == 0 && sck.ctx.Err() == nil {
			sck.reaperCond.Wait()
		}

		if sck.ctx.Err() != nil {
			return
		}

		cc := append([]*Conn{}, sck.closedConns...)
		sck.closedConns = sck.closedConns[:0]
		sck.reaperCond.L.Unlock()
		for _, c := range cc {
			sck.rmConn(c)
		}
		sck.reaperCond.L.Lock()
	}
}

var _ Socket = (*socket)(nil)
