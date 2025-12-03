package quicmq

import (
	"context"
	"crypto/tls"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

// SocketType enum
type SocketType int

const (
	PUB SocketType = iota
	SUB
	// REQ
	// REP
	// PUSH
	// PULL
	// DEALER
	// ROUTER
)

// Socket is the main interface (like ZMQ socket)
type Socket interface {
	// Sending
	Send(msg []byte) error
	SendMultipart(parts [][]byte) error

	// Receiving
	Recv() ([]byte, error)
	RecvMultipart() ([][]byte, error)

	// Binding/Connecting
	Bind(addr string) error
	Connect(addr string) error
	Disconnect(addr string) error
	Unbind(addr string) error

	// Subscription (PUB/SUB specific)
	Subscribe(topic string) error
	Unsubscribe(topic string) error

	// Options
	SetOption(opt SocketOption, value interface{}) error
	GetOption(opt SocketOption) (interface{}, error)

	// Lifecycle
	Close() error
	Context() context.Context
}

// Context manages sockets and underlying transport (like zmq_ctx)
type Context interface {
	NewSocket(socketType SocketType, opts ...Option) (Socket, error)
	Close() error
}

// Message represents a received message with metadata
type Message struct {
	Data      []byte
	Topic     string
	Timestamp time.Time
	Sender    string // peer address

	// For REQ/REP pattern
	ReplyTo string // correlation ID
}

// SocketOption enum
type SocketOption int

const (
	OptionSendTimeout SocketOption = iota
	OptionRecvTimeout
	OptionSendBuffer
	OptionRecvBuffer
	OptionLinger
	OptionReconnectInterval
	OptionHighWaterMark
)

// Option is functional option for configuration
type Option func(*socketConfig)

// parseAddr parses a QUIC address string (e.g., "quic://127.0.0.1:5555")
// Returns the network address in a format compatible with net package
func parseAddr(addr string) (string, error) {
	const prefix = "quic://"
	if len(addr) < len(prefix) || addr[:len(prefix)] != prefix {
		return "", context.DeadlineExceeded // Unsupported scheme
	}

	host := addr[len(prefix):]
	if host == "" {
		return "", context.DeadlineExceeded // Empty address
	}

	// Validate it's a valid address for net.Listen (will be checked again during Listen)
	// Quick check: must contain colon for port
	if !strings.ContainsRune(host, ':') {
		return "", context.DeadlineExceeded // Missing port
	}

	return host, nil
}

// Helper functions
func WithBind(addr string) Option {
	return func(c *socketConfig) {
		if parsed, err := parseAddr(addr); err == nil {
			udpAddr, _ := net.ResolveUDPAddr("udp", parsed)
			c.bindAddr = udpAddr
		}
	}
}

func WithConnect(addr string) Option {
	return func(c *socketConfig) {
		if parsed, err := parseAddr(addr); err == nil {
			udpAddr, _ := net.ResolveUDPAddr("udp", parsed)
			c.connectAddrs = append(c.connectAddrs, udpAddr)
		}
	}
}
func WithTransport(transport string) Option { return func(c *socketConfig) {} } // "quic", "tcp"
func WithTimeout(d time.Duration) Option {
	return func(c *socketConfig) {
		c.timeout = d
	}
}
func WithContext(ctx context.Context) Option { return func(c *socketConfig) {} }

type SocketID int

type TransportID int

type quicMQContext struct {
	sync.Mutex
	// transports indexed by bind address for efficient lookup
	transports map[string]*quic.Transport
	sockets    map[SocketID]Socket
	sockIdGen  SocketID
}

type socketConfig struct {
	addrs []net.Addr
	timeout      time.Duration
}

func (mq *quicMQContext) getNextSocketID() SocketID {
	mq.Lock()
	defer mq.Unlock()
	mq.sockIdGen++
	return mq.sockIdGen
}

func (mq *quicMQContext) getOrCreateTransport(addr net.Addr, tlsConfig *tls.Config) (*quic.Transport, error) {
	mq.Lock()
	defer mq.Unlock()

	addrStr := addr.String()
	// Check if transport already exists for this address
	if tr, ok := mq.transports[addrStr]; ok {
		return tr, nil
	}

	// Listen on UDP address
	udpConn, err := net.ListenUDP("udp", addr.(*net.UDPAddr))
	if err != nil {
		return nil, err
	}

	// Create a new Transport (all connections on this socket will be multiplexed)
	tr := &quic.Transport{
		Conn: udpConn,
	}

	// Store it for future reuse
	mq.transports[addrStr] = tr
	return tr, nil
}

func NewContext() (Context, error) {
	return &quicMQContext{
		transports: make(map[string]*quic.Transport),
		sockets:    make(map[SocketID]*Socket),
	}, nil
}

func (mq *quicMQContext) Close() error {
	return nil
}

type pubSocket struct {
	socketID SocketID
	listners *quic.Listener
	mu       sync.RWMutex
	streams  []*quic.Stream // Active subscriber streams
}

// Implement Socket interface methods for pubSocket
func (ps *pubSocket) Send(msg []byte) error                               { return nil }
func (ps *pubSocket) SendMultipart(parts [][]byte) error                  { return nil }
func (ps *pubSocket) Recv() ([]byte, error)                               { return nil, nil }
func (ps *pubSocket) RecvMultipart() ([][]byte, error)                    { return nil, nil }
func (ps *pubSocket) Bind(addr string) error                              { return nil }
func (ps *pubSocket) Connect(addr string) error                           { return nil }
func (ps *pubSocket) Disconnect(addr string) error                        { return nil }
func (ps *pubSocket) Unbind(addr string) error                            { return nil }
func (ps *pubSocket) Subscribe(topic string) error                        { return nil }
func (ps *pubSocket) Unsubscribe(topic string) error                      { return nil }
func (ps *pubSocket) SetOption(opt SocketOption, value interface{}) error { return nil }
func (ps *pubSocket) GetOption(opt SocketOption) (interface{}, error)     { return nil, nil }
func (ps *pubSocket) Close() error                                        { return nil }
func (ps *pubSocket) Context() context.Context                            { return context.Background() }

type subSocket struct {
	socketID SocketID
	mu       sync.RWMutex
	topics   map[string]struct{}
}

// Implement Socket interface methods for subSocket
func (ss *subSocket) Send(msg []byte) error                               { return nil }
func (ss *subSocket) SendMultipart(parts [][]byte) error                  { return nil }
func (ss *subSocket) Recv() ([]byte, error)                               { return nil, nil }
func (ss *subSocket) RecvMultipart() ([][]byte, error)                    { return nil, nil }
func (ss *subSocket) Bind(addr string) error                              { return nil }
func (ss *subSocket) Connect(addr string) error                           { return nil }
func (ss *subSocket) Disconnect(addr string) error                        { return nil }
func (ss *subSocket) Unbind(addr string) error                            { return nil }
func (ss *subSocket) Subscribe(topic string) error                        { return nil }
func (ss *subSocket) Unsubscribe(topic string) error                      { return nil }
func (ss *subSocket) SetOption(opt SocketOption, value interface{}) error { return nil }
func (ss *subSocket) GetOption(opt SocketOption) (interface{}, error)     { return nil, nil }
func (ss *subSocket) Close() error                                        { return nil }
func (ss *subSocket) Context() context.Context                            { return context.Background() }

func (mq *quicMQContext) NewSocket(socketType SocketType, opts ...Option) (Socket, error) {
	config := &socketConfig{}
	for _, opt := range opts {
		opt(config)
	}

	tlsConfig := generateTLSConfig()

	switch socketType {
	case PUB:
		return mq.getPubSocket(config, tlsConfig)
	case SUB:

	default:
		return nil, context.Background().Err()
	}
}

func (mq *quicMQContext) getPubSocket(config *socketConfig, tlsConfig *tls.Config) (*pubSocket, error) {
	if config.bindAddr == nil {
		return nil, context.Background().Err()
	}
	tr, err := mq.getOrCreateTransport(config.bindAddr, tlsConfig)
	if err != nil {
		return nil, err
	}

	socketID := mq.getNextSocketID()
	socket := &pubSocket{
		socketID: socketID,
	}

	listener, err := tr.Listen(tlsConfig, &quic.Config{
		MaxIdleTimeout:        30 * time.Second,
		HandshakeIdleTimeout:  5 * time.Second,
		MaxIncomingStreams:    100,
		MaxIncomingUniStreams: 100,
		KeepAlivePeriod:       15 * time.Second,
		InitialPacketSize:     1200,
	})
	if err != nil {
		return nil, err
	}
	socket.listners = listener

	mq.Lock()
	mq.sockets[socketID] = socket
	mq.Unlock()

	return socket, nil
}

func (mq *quicMQContext) getSubSocket(config *socketConfig, tlsConfig *tls.Config) (*subSocket, error) {
	if len(config.connectAddrs) == 0 {
		return nil, context.Background().Err()
	}
	tr, err := mq.getOrCreateTransport(config.connectAddrs[0], tlsConfig)
	if err != nil {
		return nil, err
	}

	socketID := mq.getNextSocketID()
	socket := &subSocket{
		socketID: socketID,
		topics:   make(map[string]struct{}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), config.timeout*time.Second)
	defer cancel()
	conn, err := tr.Dial(ctx, config.connectAddrs[0], tlsConfig, &quic.Config{)

	// Store socket reference
	mq.Lock()
	mq.sockets[socketID] = socket
	mq.Unlock()

	return socket, nil
}

func generateTLSConfig() *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"quicmq"},
	}
}
