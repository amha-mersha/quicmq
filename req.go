package quicmq

import (
	"context"
	"fmt"
	"sync"
)

// NewReq returns a new REQ QuicMQ socket.
// The returned socket value is initially unbound.
//
// REQ sockets implement a strict request-reply pattern: each Send must be
// followed by a Recv before another Send is allowed. Messages are sent in
// round-robin fashion across connected peers. An empty delimiter frame is
// prepended on send and stripped on receive to maintain ZMTP envelope
// compatibility.
func NewReq(ctx context.Context, opts ...Option) Socket {
	state := &reqState{readyToReq: true} // REQ starts ready to send
	req := &reqSocket{socket: newSocket(ctx, Req, opts...), state: state}
	req.r = newReqReader(ctx, state)
	req.w = newReqWriter(ctx, state)
	return req
}

// reqSocket is a REQ QuicMQ socket.
type reqSocket struct {
	*socket
	state *reqState
}

// Send puts the message on the outbound send queue.
func (sck *reqSocket) Send(msg Msg) error {
	sck.state.mu.Lock()
	defer sck.state.mu.Unlock()
	if !sck.state.readyToReq {
		return fmt.Errorf("quicmq: REQ socket: there is a pending request, call Recv() first")
	}
	sck.state.readyToReq = false

	ctx, cancel := context.WithTimeout(sck.ctx, sck.Timeout())
	defer cancel()
	err := sck.w.write(ctx, msg)
	if err != nil {
		sck.state.readyToReq = true
	}
	return err
}

// SendMulti puts the message on the outbound send queue as multipart.
func (sck *reqSocket) SendMulti(msg Msg) error {
	sck.state.mu.Lock()
	defer sck.state.mu.Unlock()

	if !sck.state.readyToReq {
		return fmt.Errorf("quicmq: REQ socket: there is a pending request, call Recv() first")
	}
	sck.state.readyToReq = false

	msg.multipart = true
	ctx, cancel := context.WithTimeout(sck.ctx, sck.Timeout())
	defer cancel()
	err := sck.w.write(ctx, msg)
	if err != nil {
		sck.state.readyToReq = true
	}
	return err
}

// Recv receives a complete message.
func (sck *reqSocket) Recv() (Msg, error) {
	var msg Msg

	sck.state.mu.Lock()
	defer sck.state.mu.Unlock()

	if sck.state.readyToReq {
		return msg, fmt.Errorf("quicmq: REQ socket: can't call Recv() without a pending request, call Send() first")
	}

	ctx, cancel := context.WithCancel(sck.ctx)
	defer cancel()
	err := sck.r.read(ctx, &msg)
	if err != nil {
		return msg, err
	}

	sck.state.readyToReq = true
	return msg, nil
}

// --- reqWriter: round-robin writer with envelope framing ---

type reqWriter struct {
	mu       sync.Mutex
	conns    []*Conn
	nextConn int
	state    *reqState
}

func newReqWriter(_ context.Context, state *reqState) *reqWriter {
	return &reqWriter{
		state: state,
	}
}

func (w *reqWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var err error
	for _, conn := range w.conns {
		e := conn.Close()
		if e != nil && err == nil {
			err = e
		}
	}

	w.conns = nil
	return err
}

func (w *reqWriter) addConn(c *Conn) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.conns = append(w.conns, c)
}

func (w *reqWriter) rmConn(c *Conn) {
	w.mu.Lock()
	defer w.mu.Unlock()

	curr := -1
	for i := range w.conns {
		if w.conns[i] == c {
			curr = i
			break
		}
	}

	if curr >= 0 {
		w.conns = append(w.conns[:curr], w.conns[curr+1:]...)
	}

	w.state.reset(c)
}

func (w *reqWriter) write(ctx context.Context, msg Msg) error {
	// Prepend empty delimiter frame (ZMTP envelope).
	msg.Frames = append([][]byte{nil}, msg.Frames...)

	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.conns) == 0 {
		return fmt.Errorf("quicmq: REQ write: no connections available")
	}

	var err error
	for i := 0; i < len(w.conns); i++ {
		cur := (i + w.nextConn) % len(w.conns)
		conn := w.conns[cur]
		err = conn.SendMsg(msg)
		if err == nil {
			w.nextConn = (cur + 1) % len(w.conns)
			w.state.set(conn)
			return nil
		}
	}
	return fmt.Errorf("quicmq: REQ write: all connections failed: %w", err)
}

// --- reqReader: reads reply from the connection that received the request ---

type reqReader struct {
	state *reqState
}

func newReqReader(_ context.Context, state *reqState) *reqReader {
	return &reqReader{
		state: state,
	}
}

func (r *reqReader) Close() error {
	return nil
}

func (r *reqReader) addConn(_ *Conn) {}
func (r *reqReader) rmConn(_ *Conn)  {}

func (r *reqReader) read(ctx context.Context, msg *Msg) error {
	curConn := r.state.get()
	if curConn == nil {
		return fmt.Errorf("quicmq: REQ read: no connection (send a request first)")
	}
	*msg = curConn.read()
	if msg.err != nil {
		return msg.err
	}
	// Strip the envelope delimiter: if the first frame is empty, remove it.
	if len(msg.Frames) > 1 && len(msg.Frames[0]) == 0 {
		msg.Frames = msg.Frames[1:]
	}
	return nil
}

// --- reqState: tracks which connection the last request was sent to ---

type reqState struct {
	mu         sync.Mutex
	lastConn   *Conn
	readyToReq bool
}

func (rs *reqState) set(c *Conn) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.lastConn = c
}

func (rs *reqState) reset(c *Conn) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.lastConn == c {
		rs.lastConn = nil
	}
}

func (rs *reqState) get() *Conn {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.lastConn
}

var (
	_ Socket = (*reqSocket)(nil)
	_ wpool  = (*reqWriter)(nil)
	_ rpool  = (*reqReader)(nil)
)
