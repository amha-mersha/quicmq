package quicmq

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

const (
	// DefaultSendHwm is the default high-water mark for outbound messages.
	DefaultSendHwm = 1000
)

// NewPub returns a new PUB QuicMQ socket.
// The returned socket value is initially unbound.
func NewPub(ctx context.Context, opts ...Option) Socket {
	pub := &pubSocket{socket: newSocket(ctx, Pub, opts...)}
	pub.w = newPubMWriter(pub.ctx)
	pub.r = newPubQReader(pub.ctx)
	return pub
}

// pubSocket is a PUB QuicMQ socket.
type pubSocket struct {
	*socket
}

// Recv returns an error — PUB sockets cannot receive messages.
func (*pubSocket) Recv() (Msg, error) {
	msg := Msg{err: fmt.Errorf("quicmq: PUB sockets can't recv messages")}
	return msg, msg.err
}

// SetOption sets an option for a socket.
func (pub *pubSocket) SetOption(name string, value any) error {
	err := pub.socket.SetOption(name, value)
	if err != nil {
		return err
	}

	if name != OptionHWM {
		return ErrBadProperty
	}

	hwm, ok := value.(int)
	if !ok {
		return ErrBadProperty
	}

	w := pub.w.(*pubMWriter)
	w.hwm.Store(int64(hwm))
	return nil
}

// Topics returns the sorted list of topics a socket is subscribed to.
func (pub *pubSocket) Topics() []string {
	return pub.topics()
}

// --- pubQReader: reads subscription commands from subscribers ---

type pubQReader struct {
	ctx context.Context

	mu sync.RWMutex
	rs []*Conn
	c  chan Msg

	sem *semaphore
}

func newPubQReader(ctx context.Context) *pubQReader {
	const qrsize = 10
	return &pubQReader{
		ctx: ctx,
		c:   make(chan Msg, qrsize),
		sem: newSemaphore(),
	}
}

func (q *pubQReader) Close() error {
	q.mu.RLock()
	var err error
	for _, r := range q.rs {
		e := r.Close()
		if e != nil && err == nil {
			err = e
		}
	}
	q.rs = nil
	q.mu.RUnlock()
	return err
}

func (q *pubQReader) addConn(r *Conn) {
	q.mu.Lock()
	q.sem.enable()
	q.rs = append(q.rs, r)
	q.mu.Unlock()
	go q.listen(q.ctx, r)
}

func (q *pubQReader) rmConn(r *Conn) {
	q.mu.Lock()
	defer q.mu.Unlock()

	cur := -1
	for i := range q.rs {
		if q.rs[i] == r {
			cur = i
			break
		}
	}
	if cur >= 0 {
		q.rs = append(q.rs[:cur], q.rs[cur+1:]...)
	}
}

func (q *pubQReader) read(ctx context.Context, msg *Msg) error {
	q.sem.lock(ctx)
	select {
	case <-ctx.Done():
	case *msg = <-q.c:
	}
	return msg.err
}

func (q *pubQReader) listen(ctx context.Context, r *Conn) {
	defer q.rmConn(r)
	defer r.Close()

	for {
		msg := r.read()
		select {
		case <-ctx.Done():
			return
		default:
			if msg.err != nil {
				return
			}
			switch {
			case q.isTopic(msg):
				r.subscribe(msg)
			default:
				q.c <- msg
			}
		}
	}
}

// isTopic checks if a message is a subscription command.
// Subscription frames: single frame, first byte is 0x00 (unsub) or 0x01 (sub).
func (q *pubQReader) isTopic(msg Msg) bool {
	if len(msg.Frames) != 1 {
		return false
	}
	frame := msg.Frames[0]
	if len(frame) == 0 {
		return false
	}
	return frame[0] == 0 || frame[0] == 1
}

// --- pubMWriter: topic-filtered multi-writer ---

type pubMWriter struct {
	ctx         context.Context
	mu          sync.RWMutex
	subscribers map[*Conn]chan Msg

	hwm atomic.Int64
}

func newPubMWriter(ctx context.Context) *pubMWriter {
	p := &pubMWriter{
		ctx:         ctx,
		subscribers: map[*Conn]chan Msg{},
	}
	p.hwm.Store(DefaultSendHwm)
	return p
}

func (w *pubMWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for conn, channel := range w.subscribers {
		_ = conn.Close()
		close(channel)
	}
	w.subscribers = nil
	return nil
}

func (mw *pubMWriter) addConn(w *Conn) {
	mw.mu.Lock()
	defer mw.mu.Unlock()

	c := make(chan Msg, mw.hwm.Load())
	mw.subscribers[w] = c
	go func() {
		for {
			msg, ok := <-c
			if !ok {
				break
			}
			topic := string(msg.Frames[0])
			if w.subscribed(topic) {
				_ = w.SendMsg(msg)
			}
		}
	}()
}

func (mw *pubMWriter) rmConn(w *Conn) {
	mw.mu.Lock()
	defer mw.mu.Unlock()

	if channel, ok := mw.subscribers[w]; ok {
		_ = w.Close()
		delete(mw.subscribers, w)
		close(channel)
	}
}

func (w *pubMWriter) write(ctx context.Context, msg Msg) error {
	w.mu.RLock()
	defer w.mu.RUnlock()

	for _, channel := range w.subscribers {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case channel <- msg:
		default:
			// Drop message if subscriber is slow (HWM exceeded).
		}
	}
	return nil
}

var (
	_ rpool  = (*pubQReader)(nil)
	_ wpool  = (*pubMWriter)(nil)
	_ Socket = (*pubSocket)(nil)
	_ Topics = (*pubSocket)(nil)
)
