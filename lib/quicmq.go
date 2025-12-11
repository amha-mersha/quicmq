package quicmq

import (
	"context"
	"crypto/tls"
	"errors"
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
	SetOption(opt SocketOption, value any) error
	GetOption(opt SocketOption) (any, error)

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
	ReplyTo   string // correlation ID
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

// ParseAddr parses QUIC addresses in multiple formats:
// - "quic://host:port" (default, uses UDP)
// - "quic+udp://host:port" (explicit UDP)
// - "host:port" (inferred as quic://)
// Returns a net.UDPAddr for use with QUIC transport
func ParseAddr(addr string) (*net.UDPAddr, error) {
	var host string

	// Handle various address formats
	switch {
	case strings.HasPrefix(addr, "quic+udp://"):
		host = addr[len("quic+udp://"):]
	case strings.HasPrefix(addr, "quic://"):
		host = addr[len("quic://"):]
	case strings.HasPrefix(addr, "udp://"):
		host = addr[len("udp://"):]
	case !strings.Contains(addr, "://"):
		// No scheme, assume quic://
		host = addr
	default:
		return nil, errors.New("unsupported address scheme")
	}

	if host == "" {
		return nil, errors.New("empty address")
	}

	if !strings.ContainsRune(host, ':') {
		return nil, errors.New("missing port")
	}

	udpAddr, err := net.ResolveUDPAddr("udp", host)
	if err != nil {
		return nil, err
	}

	return udpAddr, nil
}

type (
	SocketID    int
	StreamID    int
	TransportID int
)

// transports and their listeners
type TransportListeners struct {
	sync.Mutex
	listeners *quic.Listener
	transport *quic.Transport
}

// Context implementation
type quicMQContext struct {
	sync.Mutex
	sockets        map[SocketID]Socket
	sockIDGen      SocketID
	MaxSocketSlots int
}

func NewContext() (Context, error) {
	return &quicMQContext{
		sockets: make(map[SocketID]Socket, 128), // pre-allocate for efficiency
	}, nil
}

func (mq *quicMQContext) Close() error {
	mq.Lock()
	sockets := mq.sockets
	mq.sockets = nil
	mq.Unlock()

	for _, socket := range sockets {
		socket.Close()
	}
	return nil
}

func (mq *quicMQContext) getNextSocketID() (SocketID, error) {
	mq.Lock()
	defer mq.Unlock()

	if mq.MaxSocketSlots > 0 && len(mq.sockets) >= mq.MaxSocketSlots {
		return 0, errors.New("max sockets reached")
	}

	mq.sockIDGen++
	return mq.sockIDGen, nil
}

func (ps *pubSocket) Unbind(addr string) error {
	parsed, err := ParseAddr(addr)
	if err != nil {
		return err
	}

	addrStr := parsed.String()

	ps.Lock()
	defer ps.Unlock()

	if tl, exists := ps.transportListeners[addrStr]; exists {
		tl.Lock()
		tl.listeners.Close()
		tl.transport.Close()
		tl.Unlock()
		delete(ps.transportListeners, addrStr)
	}
	return nil
}

type pubSocket struct {
	socketID SocketID
	context  *quicMQContext

	sync.Mutex
	lazyPublish sync.Once

	transportListeners map[string]*TransportListeners
	timeout            time.Duration
	maxBufferSize      int
	sendTimeout        time.Duration
	sendingQueue       chan []byte
	subscriberStreams []*quic.Stream
}
func (ps *pubSocket) Bind(addr string) error {
	parsed, err := ParseAddr(addr)
	if err != nil {
		return err
	}

	ps.Lock()
	defer ps.Unlock()

	addrStr := parsed.String()
	if _, exists := ps.transportListeners[addrStr]; exists {
		return errors.New("address already bound")
	}

	// Create transport and start listening
	conn, err := net.ListenUDP(parsed.Network(), parsed)
	if err != nil {
		return err
	}
	tr := &quic.Transport{
		Conn: conn,
	}
	listener, err := tr.Listen(
		generateTLSConfig(),
		&quic.Config{},
	)
	if err != nil {
		return err
	}

	go func() {
		for {
			quicConn, err := listener.Accept(context.Background())
			if err != nil {
				return
			}
			go func() {
				for {
					stream, err := quicConn.AcceptStream(context.Background())
					if err != nil {
						return
					}
					ps.subscriberStreams = append(ps.subscriberStreams, stream)
				}
			}()
		}
	}()

	ps.transportListeners[addrStr] = &TransportListeners{
		listeners: listener,
		transport: tr,
	}
	return nil

}

func (ps *pubSocket) Send(msg []byte) error {
	if len(msg) == 0 {
		return nil
	}

	ps.Lock()
	if ps.sendingQueue == nil {
		ps.sendingQueue = make(chan []byte, 100)
	}
	select {
	case ps.sendingQueue <- msg:
	default:
		return errors.New("sending queue is full")
	}
	ps.Unlock()
	ps.lazyPublish.Do(ps.handlePublishing)
}

func (ps *pubSocket) handlePublishing() {
	for msg := range ps.sendingQueue {
		ps.Lock()
		for _, stream := range ps.subscriberStreams {
			_, err := stream.Write(msg)
			if err != nil {
				// Handle error (e.g., log it)
			}
		}
		ps.Unlock()
	}

}

func (ps *pubSocket) SendMultipart(parts [][]byte) error {
	// Not implemented yet
	// for _, part := range parts {
	// 	if err := ps.Send(part); err != nil {
	// 		return err
	// 	}
	// }
	// return nil
}

func (ps *pubSocket) Recv() ([]byte, error) {
	return nil, errors.New("publishers don't receive")
}

func (ps *pubSocket) RecvMultipart() ([][]byte, error) {
	return nil, errors.New("publishers don't receive")
}

func (ps *pubSocket) Connect(addr string) error {
	return errors.New("publishers don't connect")
}

func (ps *pubSocket) Disconnect(addr string) error {
	return errors.New("publishers don't disconnect")
}

func (ps *pubSocket) Subscribe(topic string) error {
	return errors.New("publishers don't subscribe")
}

func (ps *pubSocket) Unsubscribe(topic string) error {
	return errors.New("publishers don't unsubscribe")
}

func (ps *pubSocket) SetOption(opt SocketOption, value any) error {
	ps.Lock()
	defer ps.Unlock()

	switch opt {
	case OptionSendTimeout:
		if d, ok := value.(time.Duration); ok {
			ps.sendTimeout = d
			return nil
		} else {
			return errors.New("invalid value passed for time duration")
		}
	case OptionSendBuffer:
		if size, ok := value.(int); ok {
			if size <= 0 {
				return errors.New("send buffer size can't be of size smaller than 1.")
			}
			ps.maxBufferSize = size
			return nil
		} else {
			return errors.New("invalide value passed for buffer size")
		}
	case OptionLinger:
		if d, ok := value.(time.Duration); ok {
			ps.timeout = d
			return nil
		}
	}
	return errors.New("invalid option")
}

func (ps *pubSocket) GetOption(opt SocketOption) (any, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	switch opt {
	case OptionSendTimeout:
		return ps.sendTimeout, nil
	case OptionSendBuffer:
		return ps.maxBufferSize, nil
	case OptionLinger:
		return ps.timeout, nil
	}
	return nil, errors.New("invalid option")
}

func (ps *pubSocket) Close() error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.listeners != nil {
		ps.listeners.Close()
	}

	for _, sb := range ps.activeStreams {
		if sb.stream != nil {
			sb.stream.Close()
		}
	}

	ps.activeStreams = make(map[StreamID]*StreamBuffer)
	return nil
}

func (ps *pubSocket) Context() context.Context {
	return context.Background()
}

// ===== SUB SOCKET =====
// subSocket contains ALL configuration fields directly (no separate config struct)
type subSocket struct {
	socketID SocketID
	context  *quicMQContext // reference to parent context for transport lookup
	mu       sync.RWMutex

	// Configuration fields (embedded from what was socketConfig)
	connectAddrs  []*net.UDPAddr
	timeout       time.Duration
	maxBufferSize int
	recvTimeout   time.Duration

	// State
	topics        map[string]struct{}
	activeStreams map[StreamID]*StreamBuffer
}

func (ss *subSocket) Send(msg []byte) error {
	return errors.New("subscribers don't send")
}

func (ss *subSocket) SendMultipart(parts [][]byte) error {
	return errors.New("subscribers don't send")
}

func (ss *subSocket) Recv() ([]byte, error) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	if len(ss.activeStreams) == 0 {
		return nil, errors.New("no active streams")
	}

	for _, sb := range ss.activeStreams {
		if len(sb.buffer) > 0 {
			msg := sb.buffer[0]
			sb.buffer = sb.buffer[1:]
			return msg, nil
		}
	}

	return nil, errors.New("no messages available")
}

func (ss *subSocket) RecvMultipart() ([][]byte, error) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	var parts [][]byte
	for _, sb := range ss.activeStreams {
		parts = append(parts, sb.buffer...)
		sb.buffer = [][]byte{}
	}

	if len(parts) == 0 {
		return nil, errors.New("no messages available")
	}
	return parts, nil
}

func (ss *subSocket) Bind(addr string) error {
	return errors.New("subscribers don't bind")
}

func (ss *subSocket) Connect(addr string) error {
	parsed, err := parseAddr(addr)
	if err != nil {
		return err
	}

	udpAddr, err := net.ResolveUDPAddr("udp", parsed)
	if err != nil {
		return err
	}

	ss.mu.Lock()
	defer ss.mu.Unlock()

	ss.connectAddrs = append(ss.connectAddrs, udpAddr)
	return nil
}

func (ss *subSocket) Disconnect(addr string) error {
	parsed, err := parseAddr(addr)
	if err != nil {
		return err
	}

	ss.mu.Lock()
	defer ss.mu.Unlock()

	var newAddrs []*net.UDPAddr
	for _, a := range ss.connectAddrs {
		if a.String() != parsed {
			newAddrs = append(newAddrs, a)
		}
	}
	ss.connectAddrs = newAddrs
	return nil
}

func (ss *subSocket) Unbind(addr string) error {
	return errors.New("subscribers don't unbind")
}

func (ss *subSocket) Subscribe(topic string) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	if ss.topics == nil {
		ss.topics = make(map[string]struct{})
	}
	ss.topics[topic] = struct{}{}
	return nil
}

func (ss *subSocket) Unsubscribe(topic string) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	delete(ss.topics, topic)
	return nil
}

func (ss *subSocket) SetOption(opt SocketOption, value any) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	switch opt {
	case OptionRecvTimeout:
		if d, ok := value.(time.Duration); ok {
			ss.recvTimeout = d
			return nil
		}
	case OptionRecvBuffer:
		if size, ok := value.(int); ok {
			ss.maxBufferSize = size
			return nil
		}
	case OptionLinger:
		if d, ok := value.(time.Duration); ok {
			ss.timeout = d
			return nil
		}
	}
	return errors.New("invalid option")
}

func (ss *subSocket) GetOption(opt SocketOption) (any, error) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	switch opt {
	case OptionRecvTimeout:
		return ss.recvTimeout, nil
	case OptionRecvBuffer:
		return ss.maxBufferSize, nil
	case OptionLinger:
		return ss.timeout, nil
	}
	return nil, errors.New("invalid option")
}

func (ss *subSocket) Close() error {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	for _, sb := range ss.activeStreams {
		if sb.stream != nil {
			sb.stream.Close()
		}
	}

	ss.activeStreams = make(map[StreamID]*StreamBuffer)
	ss.topics = make(map[string]struct{})
	return nil
}

func (ss *subSocket) Context() context.Context {
	return context.Background()
}

// ===== FACTORY METHODS =====

// Option is functional option for configuration (applies to specific socket at creation time)
type Option func(Socket) error

func WithBind(addr string) Option {
	return func(s Socket) error {
		return s.Bind(addr)
	}
}

func WithConnect(addr string) Option {
	return func(s Socket) error {
		return s.Connect(addr)
	}
}

func WithTimeout(d time.Duration) Option {
	return func(s Socket) error {
		return s.SetOption(OptionLinger, d)
	}
}

func WithRecvBuffer(size int) Option {
	return func(s Socket) error {
		return s.SetOption(OptionRecvBuffer, size)
	}
}

func WithSendBuffer(size int) Option {
	return func(s Socket) error {
		return s.SetOption(OptionSendBuffer, size)
	}
}

func (mq *quicMQContext) NewSocket(socketType SocketType, opts ...Option) (Socket, error) {
	socketID, err := mq.getNextSocketID()
	if err != nil {
		return nil, err
	}

	var socket Socket

	switch socketType {
	case PUB:
		socket = &pubSocket{
			socketID:      socketID,
			context:       mq,
			timeout:       30 * time.Second,
			maxBufferSize: 100,
			sendTimeout:   5 * time.Second,
			sendingQueue:  make(chan []byte, 100),
			transportListeners: make(map[string]*TransportListeners),
			subscriberStreams: make([]*quic.Stream, 0),
		}

	case SUB:
		socket = &subSocket{
			socketID:      socketID,
			context:       mq,
			timeout:       30 * time.Second,
			maxBufferSize: 100,
			recvTimeout:   5 * time.Second,
			topics:        make(map[string]struct{}),
			activeStreams: make(map[StreamID]*StreamBuffer),
		}

	default:
		return nil, errors.New("unsupported socket type")
	}

	// Apply options
	for _, opt := range opts {
		if err := opt(socket); err != nil {
			socket.Close()
			return nil, err
		}
	}

	// Store in context
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
