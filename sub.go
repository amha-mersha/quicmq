package quicmq

import (
	"context"
	"sort"
	"sync"
)

// NewSub returns a new SUB QuicMQ socket.
// The returned socket value is initially unbound.
func NewSub(ctx context.Context, opts ...Option) Socket {
	sub := &subSocket{socket: newSocket(ctx, Sub, opts...)}
	sub.r = newQReader(sub.ctx)
	sub.subs = make(map[string]struct{})
	sub.onConnAdded = func(c *Conn) {
		for _, topic := range sub.Topics() {
			_ = c.SendMsg(NewMsg(append([]byte{1}, topic...)))
		}
	}
	return sub
}

// subSocket is a SUB QuicMQ socket.
type subSocket struct {
	*socket

	mu   sync.RWMutex
	subs map[string]struct{}
}

// SetOption sets an option for a socket.
// Supports OptionSubscribe and OptionUnsubscribe.
func (sub *subSocket) SetOption(name string, value interface{}) error {
	err := sub.socket.SetOption(name, value)
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

	sub.socket.mu.RLock()
	if len(sub.conns) > 0 {
		err = sub.Send(NewMsg(topic))
	}
	sub.socket.mu.RUnlock()
	return err
}

// Topics returns the sorted list of topics this socket is subscribed to.
func (sub *subSocket) Topics() []string {
	sub.mu.RLock()
	topics := make([]string, 0, len(sub.subs))
	for topic := range sub.subs {
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
		delete(sub.subs, topic)
	case 1:
		sub.subs[topic] = struct{}{}
	}
	sub.mu.Unlock()
}

var (
	_ Socket = (*subSocket)(nil)
	_ Topics = (*subSocket)(nil)
)
