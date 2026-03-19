package quicmq

import (
	"context"
	"net"
	"sort"
	"sync"
)

// NewSub returns a new SUB QuicMQ socket.
// The returned socket value is initially unbound.
func NewSub(ctx context.Context, opts ...Option) Socket {
	sub := &subSocket{sck: newSocket(ctx, Sub, opts...)}
	sub.sck.r = newQReader(sub.sck.ctx)
	sub.topics = make(map[string]struct{})
	sub.sck.onConnAdded = func(c *Conn) {
		for _, topic := range sub.Topics() {
			_ = c.SendMsg(NewMsg(append([]byte{1}, topic...)))
		}
	}
	return sub
}

// subSocket is a SUB QuicMQ socket.
type subSocket struct {
	sck *socket

	mu     sync.RWMutex
	topics map[string]struct{}
}

// Close closes the open Socket.
func (sub *subSocket) Close() error {
	return sub.sck.Close()
}

// Send puts the message on the outbound send queue.
func (sub *subSocket) Send(msg Msg) error {
	return sub.sck.Send(msg)
}

// SendMulti puts the message on the outbound send queue as a multipart message.
func (sub *subSocket) SendMulti(msg Msg) error {
	return sub.sck.SendMulti(msg)
}

// Recv receives a complete message.
func (sub *subSocket) Recv() (Msg, error) {
	return sub.sck.Recv()
}

// Listen binds a local endpoint to the Socket.
func (sub *subSocket) Listen(ep string) error {
	return sub.sck.Listen(ep)
}

// Dial connects a remote endpoint to the Socket.
func (sub *subSocket) Dial(ep string) error {
	return sub.sck.Dial(ep)
}

// Type returns the type of this Socket.
func (sub *subSocket) Type() SocketType {
	return sub.sck.Type()
}

// Addr returns the listener's address.
func (sub *subSocket) Addr() net.Addr {
	return sub.sck.Addr()
}

// GetOption retrieves an option for a socket.
func (sub *subSocket) GetOption(name string) (interface{}, error) {
	return sub.sck.GetOption(name)
}

// SetOption sets an option for a socket.
// Supports OptionSubscribe and OptionUnsubscribe.
func (sub *subSocket) SetOption(name string, value interface{}) error {
	err := sub.sck.SetOption(name, value)
	if err != nil {
		return err
	}

	var topic []byte

	switch name {
	case OptionSubscribe:
		k := value.(string)
		sub.subscribe(k, 1)
		topic = append([]byte{1}, k...)

	case OptionUnsubscribe:
		k := value.(string)
		topic = append([]byte{0}, k...)
		sub.subscribe(k, 0)

	default:
		return ErrBadProperty
	}

	sub.sck.mu.RLock()
	if len(sub.sck.conns) > 0 {
		err = sub.Send(NewMsg(topic))
	}
	sub.sck.mu.RUnlock()
	return err
}

// Topics returns the sorted list of topics this socket is subscribed to.
func (sub *subSocket) Topics() []string {
	sub.mu.RLock()
	topics := make([]string, 0, len(sub.topics))
	for topic := range sub.topics {
		topics = append(topics, topic)
	}
	sub.mu.RUnlock()
	sort.Strings(topics)
	return topics
}

func (sub *subSocket) subscribe(topic string, v int) {
	sub.mu.Lock()
	switch v {
	case 0:
		delete(sub.topics, topic)
	case 1:
		sub.topics[topic] = struct{}{}
	}
	sub.mu.Unlock()
}

var (
	_ Socket = (*subSocket)(nil)
	_ Topics = (*subSocket)(nil)
)
