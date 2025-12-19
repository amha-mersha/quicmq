package quicmq

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
)

const (
	ERR_ADDR_AREADY_BOUND       string = "address is already bound"
	ERR_NOT_CONNECTECD          string = "address has not been connected before"
	ERR_SENDING_QUEUE_FULL      string = "sending queue is full"
	ERR_INVALID_OPTION_VALUE    string = "invalid option value"
	ERR_SOCKET_CLOSED           string = "socket is already closed"
	ERR_TIMEOUT                 string = "operation timed out"
	ERR_CONNECTION_BEING_CLOSED string = "connection is being closed"
	ERR_TOPIC_ALREAD_SUBSCRIBED string = "topic is already subscribed by this socket"
	ERR_TOPIC_DOES_NOT_EXIST    string = "topic does not exist"
)

const (
	CONN_CLOSED string = "connection is closed"
)

type QuicContext struct {
	sync.Mutex
	sockets        map[SocketID]Socket
	sockIDGen      SocketID
	MaxSocketSlots int
}

func NewQuicContext() (*QuicContext, error) {
	return &QuicContext{
		sockets: make(map[SocketID]Socket),
	}, nil
}

func (mq *QuicContext) Close() error {
	mq.Lock()
	sockets := mq.sockets
	mq.sockets = nil
	mq.Unlock()

	for _, socket := range sockets {
		socket.Close()
	}
	return nil
}

func (mq *QuicContext) getNextSocketID() (SocketID, error) {
	mq.Lock()
	defer mq.Unlock()

	if mq.MaxSocketSlots > 0 && len(mq.sockets) >= mq.MaxSocketSlots {
		return 0, errors.New("max sockets reached")
	}

	mq.sockIDGen++
	return mq.sockIDGen, nil
}

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
	// Context() context.Context
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

type transportConnection struct {
	mu        sync.Mutex
	transport *quic.Transport
	conn      *quic.Conn
}

type baseSocket struct {
	mu            sync.Mutex
	transportConn map[string]*transportConnection
	socketID      SocketID
	context       *QuicContext
	maxBufferSize int
	closed        atomic.Bool
}

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

type pubSocket struct {
	*baseSocket
	lazyPublish       sync.Once
	sendTimeout       time.Duration
	subscriberStreams map[quic.StreamID]*quic.Stream
	writeQueue        chan []byte
}

func (ps *pubSocket) Bind(addr string) error {
	if ps.closed.Load() {
		return errors.New(ERR_SOCKET_CLOSED)
	}

	parsed, err := ParseAddr(addr)
	if err != nil {
		return err
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	addrStr := parsed.String()
	if _, exists := ps.transportConn[addrStr]; exists {
		return errors.New(ERR_ADDR_AREADY_BOUND)
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
			ps.transportConn[addrStr].mu.Lock()
			ps.transportConn[addrStr].conn = quicConn
			ps.transportConn[addrStr].mu.Unlock()
			if err != nil {
				return
			}
			go func() {
				for {
					stream, err := quicConn.AcceptStream(context.Background())
					if err != nil {
						return
					}
					ps.subscriberStreams[stream.StreamID()] = stream
				}
			}()
		}
	}()

	ps.transportConn[addrStr].transport = tr
	return nil
}

func (ps *pubSocket) Unbind(addr string) error {
	if ps.closed.Load() {
		return errors.New(ERR_SOCKET_CLOSED)
	}

	parsed, err := ParseAddr(addr)
	if err != nil {
		return err
	}

	addrStr := parsed.String()

	ps.mu.Lock()
	defer ps.mu.Unlock()

	if transportConn, exists := ps.transportConn[addrStr]; exists {
		err := transportConn.transport.Close()
		if err != nil {
			return err
		}
		quicConn := transportConn.conn
		err = quicConn.CloseWithError(quic.ApplicationErrorCode(quic.ApplicationErrorErrorCode), ERR_CONNECTION_BEING_CLOSED)
		if err != nil {
			return err
		}
		delete(ps.transportConn, addrStr)
	}
	return nil
}

func (ps *pubSocket) Send(msg []byte) error {
	if ps.closed.Load() {
		return errors.New(ERR_SOCKET_CLOSED)
	}

	if len(msg) == 0 {
		return nil
	}

	ps.mu.Lock()
	if ps.writeQueue == nil {
		ps.writeQueue = make(chan []byte, 100)
	}
	select {
	case ps.writeQueue <- msg:
	default:
		return errors.New(ERR_SENDING_QUEUE_FULL)
	}
	ps.mu.Unlock()
	ps.lazyPublish.Do(
		func() { go ps.handlePublishing() },
	)
	return nil
}

func (ps *pubSocket) handlePublishing() {
	for msg := range ps.writeQueue {
		ps.mu.Lock()
		for _, stream := range ps.subscriberStreams {
			_, err := stream.Write(msg)
			if err != nil {
				if !errors.Is(err, &quic.StreamError{}) {
					delete(ps.subscriberStreams, stream.StreamID())
				} else {
					// Handle stream error (e.g., log it)
				}
			}
		}
		ps.mu.Unlock()
	}

}

func (ps *pubSocket) SendMultipart(parts [][]byte) error {
	// Not implemented yet
	// for _, part := range parts {
	// 	if err := ps.Send(part); err != nil {
	// 		return err
	// 	}
	// }
	return nil
}

func (ps *pubSocket) Recv() ([]byte, error) {
	if ps.closed.Load() {
		return nil, errors.New(ERR_SOCKET_CLOSED)
	}
	return nil, errors.New("publishers don't receive")
}

func (ps *pubSocket) RecvMultipart() ([][]byte, error) {
	if ps.closed.Load() {
		return nil, errors.New(ERR_SOCKET_CLOSED)
	}
	return nil, errors.New("publishers don't receive")
}

func (ps *pubSocket) Connect(addr string) error {
	return errors.New("publishers don't connect")
}

func (ps *pubSocket) Disconnect(addr string) error {
	if ps.closed.Load() {
		return errors.New(ERR_SOCKET_CLOSED)
	}
	return errors.New("publishers don't disconnect")
}

func (ps *pubSocket) Subscribe(topic string) error {
	if ps.closed.Load() {
		return errors.New(ERR_SOCKET_CLOSED)
	}
	return errors.New("publishers don't subscribe")
}

func (ps *pubSocket) Unsubscribe(topic string) error {
	return errors.New("publishers don't unsubscribe")
}

func (ps *pubSocket) SetOption(opt SocketOption, value any) error {
	if ps.closed.Load() {
		return errors.New(ERR_SOCKET_CLOSED)
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()

	switch opt {
	case OptionSendTimeout:
		if d, ok := value.(time.Duration); ok {
			ps.sendTimeout = d
			return nil
		} else {
			return errors.New(ERR_INVALID_OPTION_VALUE)
		}
	case OptionSendBuffer:
		if size, ok := value.(int); ok {
			if size <= 0 {
				return errors.New(fmt.Sprintf("%s : buffer size must be greater than 0", ERR_INVALID_OPTION_VALUE))
			}
			ps.maxBufferSize = size
			return nil
		} else {
			return errors.New(ERR_INVALID_OPTION_VALUE)
		}
	case OptionLinger:
		if d, ok := value.(time.Duration); ok {
			ps.sendTimeout = d
			return nil
		} else {
			return errors.New(ERR_INVALID_OPTION_VALUE)
		}
	}
	return errors.New(ERR_INVALID_OPTION_VALUE)
}

func (ps *pubSocket) GetOption(opt SocketOption) (any, error) {
	if ps.closed.Load() {
		return nil, errors.New(ERR_SOCKET_CLOSED)
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()

	switch opt {
	case OptionSendTimeout:
		return ps.sendTimeout, nil
	case OptionSendBuffer:
		return ps.maxBufferSize, nil
	case OptionLinger:
		return ps.sendTimeout, nil
	}
	return nil, errors.New(ERR_INVALID_OPTION_VALUE)
}

func (ps *pubSocket) Close() error {
	if !ps.closed.CompareAndSwap(false, true) {
		return errors.New(ERR_SOCKET_CLOSED)
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	for _, stream := range ps.subscriberStreams {
		if err := stream.Close(); err != nil {
			return err
		}
	}

	for _, transportConn := range ps.transportConn {
		// lock per transportConn while we close its resources
		transportConn.mu.Lock()

		if transportConn.conn != nil {
			if err := transportConn.conn.CloseWithError(0, CONN_CLOSED); err != nil {
				transportConn.mu.Unlock()
				return err
			}
		}

		if transportConn.transport != nil {
			if err := transportConn.transport.Close(); err != nil {
				transportConn.mu.Unlock()
				return err
			}
		}

		transportConn.mu.Unlock()
	}

	delete(ps.context.sockets, ps.socketID)
	return nil
}

type subSocket struct {
	*baseSocket
	recvQueue      chan []byte
	recvTimeout    time.Duration
	topicToStreams map[string]*quic.Stream
}

func (ss *subSocket) Send(msg []byte) error {
	if ss.closed.Load() {
		return errors.New(ERR_SOCKET_CLOSED)
	}
	return errors.New("subscribers don't send")
}

func (ss *subSocket) SendMultipart(parts [][]byte) error {
	if ss.closed.Load() {
		return errors.New(ERR_SOCKET_CLOSED)
	}
	return errors.New("subscribers don't send")
}

func (ss *subSocket) Recv() ([]byte, error) {
	if ss.closed.Load() {
		return nil, errors.New(ERR_SOCKET_CLOSED)
	}
	select {
	case msg := <-ss.recvQueue:
		return msg, nil
	case <-time.After(ss.recvTimeout):
		return nil, errors.New(ERR_TIMEOUT)
	}
}

func (ss *subSocket) RecvMultipart() ([][]byte, error) {
	if ss.closed.Load() {
		return nil, errors.New(ERR_SOCKET_CLOSED)
	}
	return nil, errors.New("not implemented yet")
}

func (ss *subSocket) Bind(addr string) error {
	if ss.closed.Load() {
		return errors.New(ERR_SOCKET_CLOSED)
	}
	return errors.New("subscribers don't bind")
}

func (ss *subSocket) Connect(addr string) error {
	if ss.closed.Load() {
		return errors.New(ERR_SOCKET_CLOSED)
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if _, ok := ss.transportConn[addr]; ok {
		return errors.New(ERR_ADDR_AREADY_BOUND)
	}
	tlsConf := generateTLSConfig()
	netAddr, err := ParseAddr(addr)
	if err != nil {
		return err
	}

	udpConn, err := net.ListenUDP(netAddr.Network(), netAddr)
	if err != nil {
		return err
	}

	transport := quic.Transport{
		Conn: udpConn,
	}

	quicConn, err := transport.Dial(context.Background(), netAddr, tlsConf, nil)
	if err != nil {
		return err
	}

	addrStr := netAddr.String()
	transConn := ss.transportConn[addrStr]
	transConn.mu.Lock()
	defer transConn.mu.Unlock()
	transConn.transport = &transport
	transConn.conn = quicConn
	return nil
}

func (ss *subSocket) Disconnect(addr string) error {
	if ss.closed.Load() {
		return errors.New(ERR_SOCKET_CLOSED)
	}
	parsed, err := ParseAddr(addr)
	if err != nil {
		return err
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	addrStr := parsed.String()
	if transConn, ok := ss.transportConn[addrStr]; ok {
		transConn.mu.Lock()
		defer transConn.mu.Unlock()
		if err := transConn.transport.Close(); err != nil {
			return err
		}
		if err := transConn.conn.CloseWithError(quic.ApplicationErrorCode(quic.ApplicationErrorErrorCode), ERR_CONNECTION_BEING_CLOSED); err != nil {
			return err
		}
		delete(ss.transportConn, addrStr)
	} else {
		return errors.New(ERR_NOT_CONNECTECD)
	}
	return nil
}

func (ss *subSocket) Unbind(addr string) error {
	if ss.closed.Load() {
		return errors.New(ERR_SOCKET_CLOSED)
	}
	return errors.New("subscribers don't unbind")
}

func (ss *subSocket) Subscribe(topic string) error {
	if ss.closed.Load() {
		return errors.New(ERR_SOCKET_CLOSED)
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if _, ok := ss.topicToStreams[topic]; ok {
		return errors.New(ERR_TOPIC_ALREAD_SUBSCRIBED)
	} else {
		for _, tranConn := range ss.transportConn {
			tranConn.mu.Lock()
			defer tranConn.mu.Unlock()
			quicConn := tranConn.conn
			stream, err := quicConn.OpenStreamSync(context.Background()) // probably need timeout context #TODO
			if err != nil {
				return err
			}
			ss.topicToStreams[topic] = stream
		}
	}
	return nil
}

func (ss *subSocket) Unsubscribe(topic string) error {
	if ss.closed.Load() {
		return errors.New(ERR_SOCKET_CLOSED)
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()

	if stream, ok := ss.topicToStreams[topic]; ok {
		if err := stream.Close(); err != nil {
			return err
		}
		delete(ss.topicToStreams, topic)
	} else {
		return errors.New(ERR_TOPIC_DOES_NOT_EXIST)
	}
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
			ss.recvTimeout = d
			return nil
		}
	}
	return errors.New("invalid option")
}

func (ss *subSocket) GetOption(opt SocketOption) (any, error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	switch opt {
	case OptionRecvTimeout:
		return ss.recvTimeout, nil
	case OptionRecvBuffer:
		return ss.maxBufferSize, nil
	case OptionLinger:
		return ss.recvTimeout, nil
	}
	return nil, errors.New("invalid option")
}

func (ss *subSocket) Close() error {
	if !ss.closed.CompareAndSwap(false, true) {
		return errors.New(ERR_SOCKET_CLOSED)
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	for _, stream := range ss.topicToStreams {
		if err := stream.Close(); err != nil {
			return err
		}
	}

	for _, transportConn := range ss.transportConn {
		transportConn.mu.Lock()
		defer transportConn.mu.Unlock()
		if err := transportConn.conn.CloseWithError(0, CONN_CLOSED); err != nil {
			return err
		}
		if err := transportConn.transport.Close(); err != nil {
			return err
		}
	}
	delete(ss.context.sockets, ss.socketID)
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

func (mq *QuicContext) NewSocket(socketType SocketType, opts ...Option) (Socket, error) {
	socketID, err := mq.getNextSocketID()
	if err != nil {
		return nil, err
	}

	var socket Socket

	switch socketType {
	case PUB:
		socket = &pubSocket{
			baseSocket: &baseSocket{
				socketID:      socketID,
				context:       mq,
				maxBufferSize: 100,
				transportConn: make(map[string]*transportConnection),
			},
			sendTimeout:       30 * time.Second,
			subscriberStreams: make(map[quic.StreamID]*quic.Stream),
		}

	case SUB:
		socket = &subSocket{
			baseSocket: &baseSocket{
				socketID:      socketID,
				context:       mq,
				maxBufferSize: 100,
				transportConn: make(map[string]*transportConnection),
			},
			recvTimeout:    30 * time.Second,
			recvQueue:      make(chan []byte, 100),
			topicToStreams: make(map[string]*quic.Stream),
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
