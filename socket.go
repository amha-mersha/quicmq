package quicmq

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

const (
	defaultRetry      = 250 * time.Millisecond
	defaultTimeout    = 5 * time.Minute
	defaultMaxRetries = 10
)

var (
	errInvalidAddress = errors.New("quicmq: invalid address")
	ErrBadProperty    = errors.New("quicmq: bad property")
)

// socket is the base implementation shared by all socket types.
type socket struct {
	ep         string
	typ        SocketType
	retry      time.Duration
	maxRetries int
	log        *log.Logger
	subTopics  func() []string // callback to get SUB topics for re-subscription
	timeout    time.Duration

	// TLS configs (QUIC-specific, but kept here for convenience).
	tlsCfg       *tls.Config
	clientTlsCfg *tls.Config

	mu    sync.RWMutex
	conns []*Conn
	r     rpool
	w     wpool

	props map[string]interface{}

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
		typ:        sockType,
		retry:      defaultRetry,
		maxRetries: defaultMaxRetries,
		timeout:    defaultTimeout,
		conns:      nil,
		r:          newQReader(ctx),
		w:          newMWriter(ctx),
		props:      make(map[string]interface{}),
		ctx:        ctx,
		cancel:     cancel,
		reaperCond: sync.NewCond(&sync.Mutex{}),
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
	network, addr, err := splitAddr(endpoint)
	if err != nil {
		return err
	}

	trans, ok := drivers.get(network)
	if !ok {
		return UnknownTransportError{Name: network}
	}

	l, err := trans.Listen(sck.ctx, addr)
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
func (sck *socket) Dial(endpoint string) error {
	sck.ep = endpoint
	network, addr, err := splitAddr(endpoint)
	if err != nil {
		return err
	}

	trans, ok := drivers.get(network)
	if !ok {
		return UnknownTransportError{Name: network}
	}

	var (
		conn    net.Conn
		retries = 0
	)

connect:
	conn, err = trans.Dial(sck.ctx, addr)
	if err != nil {
		if (sck.maxRetries == -1 || retries < sck.maxRetries) && sck.ctx.Err() == nil {
			retries++
			time.Sleep(sck.retry)
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
	// resend subscriptions for topics if there are any
	if sck.subTopics != nil {
		for _, topic := range sck.subTopics() {
			_ = sck.Send(NewMsg(append([]byte{1}, topic...)))
		}
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
func (sck *socket) GetOption(name string) (interface{}, error) {
	v, ok := sck.props[name]
	if !ok {
		return nil, ErrBadProperty
	}
	return v, nil
}

// SetOption sets an option for a socket.
func (sck *socket) SetOption(name string, value interface{}) error {
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
