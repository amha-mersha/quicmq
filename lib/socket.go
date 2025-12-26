package quicmq

import (
	"sync"
	"sync/atomic"
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
