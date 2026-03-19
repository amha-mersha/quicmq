// Package quicmq implements ZeroMQ-style messaging patterns over QUIC transport.
//
// QuicMQ provides broker-less pub/sub messaging with built-in TLS encryption
// via QUIC. The API follows go-zeromq/zmq4 patterns: sockets are created with
// simple constructors, no explicit context object is required.
//
// For more information, see https://github.com/amha-mersha/quicmq.
package quicmq

import (
	"bytes"
	"fmt"
	"net"
)

// Socket represents a QuicMQ socket.
type Socket interface {
	// Close closes the open Socket.
	Close() error

	// Send puts the message on the outbound send queue.
	// Send blocks until the message can be queued or the send deadline expires.
	Send(msg Msg) error

	// SendMulti puts the message on the outbound send queue.
	// SendMulti blocks until the message can be queued or the send deadline
	// expires. The message will be sent as a multipart message.
	SendMulti(msg Msg) error

	// Recv receives a complete message.
	Recv() (Msg, error)

	// Listen binds a local endpoint to the Socket.
	Listen(ep string) error

	// Dial connects a remote endpoint to the Socket.
	Dial(ep string) error

	// Type returns the type of this Socket (PUB, SUB, etc.)
	Type() SocketType

	// Addr returns the listener's address. It returns nil if the socket isn't
	// a listener.
	Addr() net.Addr

	// GetOption retrieves an option for a socket.
	GetOption(name string) (interface{}, error)

	// SetOption sets an option for a socket.
	SetOption(name string, value interface{}) error
}

// Topics is an interface that wraps the basic Topics method.
type Topics interface {
	// Topics returns the sorted list of topics a socket is subscribed to.
	Topics() []string
}

// SocketType is a QuicMQ socket type.
type SocketType string

const (
	Pub SocketType = "PUB" // a PUB socket
	Sub SocketType = "SUB" // a SUB socket
	// Future socket types:
	// Req SocketType = "REQ"
	// Rep SocketType = "REP"
)

// IsCompatible checks whether two sockets are compatible and thus
// can be connected together.
func (sck SocketType) IsCompatible(peer SocketType) bool {
	switch sck {
	case Pub:
		return peer == Sub
	case Sub:
		return peer == Pub
	default:
		return false
	}
}

// Msg is a message, possibly composed of multiple frames.
type Msg struct {
	Frames    [][]byte
	multipart bool
	err       error
}

// NewMsg creates a new Msg with a single frame.
func NewMsg(frame []byte) Msg {
	return Msg{Frames: [][]byte{frame}}
}

// NewMsgFrom creates a new Msg from multiple frames.
func NewMsgFrom(frames ...[]byte) Msg {
	return Msg{Frames: frames}
}

// NewMsgString creates a new Msg with a single string frame.
func NewMsgString(frame string) Msg {
	return NewMsg([]byte(frame))
}

// NewMsgFromString creates a new Msg from a slice of strings.
func NewMsgFromString(frames []string) Msg {
	msg := Msg{Frames: make([][]byte, len(frames))}
	for i, frame := range frames {
		msg.Frames[i] = append(msg.Frames[i], []byte(frame)...)
	}
	return msg
}

// Err returns the error associated with this message, if any.
func (msg Msg) Err() error {
	return msg.err
}

// Bytes returns the concatenated content of all its frames.
func (msg Msg) Bytes() []byte {
	buf := make([]byte, 0, msg.size())
	for _, frame := range msg.Frames {
		buf = append(buf, frame...)
	}
	return buf
}

func (msg Msg) size() int {
	n := 0
	for _, frame := range msg.Frames {
		n += len(frame)
	}
	return n
}

// String returns a human-readable representation of the message.
func (msg Msg) String() string {
	buf := new(bytes.Buffer)
	buf.WriteString("Msg{Frames:{")
	for i, frame := range msg.Frames {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(buf, "%q", frame)
	}
	buf.WriteString("}}")
	return buf.String()
}

// Clone returns a deep copy of the message.
func (msg Msg) Clone() Msg {
	o := Msg{Frames: make([][]byte, len(msg.Frames))}
	for i, frame := range msg.Frames {
		o.Frames[i] = make([]byte, len(frame))
		copy(o.Frames[i], frame)
	}
	return o
}
