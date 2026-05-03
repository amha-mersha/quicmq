package quicmq

import (
	"context"
	"fmt"
	"sync"
)

func NewReq(ctx context.Context, opts ...Option) Socket {
	req := &reqSocket{socket: newSocket(ctx, Req, opts...)}
	req.r = newQReader(ctx)
	req.w = newMWriter(ctx)
	req.state = &reqState{}
	return req
}

// REQ QuicMQ Socket
type reqSocket struct {
	*socket
	state *reqState
}

// Send puts the message on the outbound send queue.
func (sck *reqSocket) Send(msg Msg) error {
	sck.state.mu.Lock()
	defer sck.state.mu.Unlock()
	if !sck.state.readyToReq {
		return fmt.Errorf("zmtp: there is a pending request, can't send again. call Recv() first.")
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
		return fmt.Errorf("zmtp: there is a pending request, can't send again. call Recv() first.")
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
		return msg, fmt.Errorf("zmtp: can't call Recv() consequently, send a request first: Send()")
	}
	sck.state.readyToReq = true

	ctx, cancel := context.WithCancel(sck.ctx)
	defer cancel()
	err := sck.r.read(ctx, &msg)

	if err != nil {
		sck.state.readyToReq = false
	}
	return msg, err
}

// Request Mulitple Writter
type reqMWritter struct {
	mu       sync.RWMutex
	conns    []*Conn
	nextConn int
	state    *reqState
}

func newReqMWritter(state *reqState) *reqMWritter {
	return &reqMWritter{
		state: state,
	}
}

func (w *reqMWritter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var err error
	for _, conn := range w.conns {
		e := conn.Close()
		if e != nil && err != nil {
			err = e
		}
	}

	w.conns = nil
	return err
}

func (w *reqMWritter) addConn(c *Conn) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.conns = append(w.conns, c)
}

func (w *reqMWritter) rmConn(c *Conn) {
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

func (w *reqMWritter) write(ctx context.Context, msg Msg) error {
	msg.Frames = append([][]byte{nil}, msg.Frames...)

	w.mu.Lock()
	defer w.mu.Unlock()

	var err error
	for i := 0; i < len(w.conns); i++ {
		curr := (i + w.nextConn) % len(w.conns)
		conn := w.conns[curr]
		err = conn.SendMsg(msg)
		if err != nil {
			w.nextConn = (curr + 1) % len(w.conns)
			w.state.set(conn)
		}
	}
	return fmt.Errorf("quicmq: error write on connection %w", err)

}

// Request Queue Reader
type reqQRreader struct {
	state *reqState
}

func (r *reqQRreader) Close() error {
}
func (r *reqQRreader) addConn(r *Conn)                          {}
func (r *reqQRreader) rmConn(r *Conn)                           {}
func (r *reqQRreader) read(ctx context.Context, msg *Msg) error {}

// Request State
type reqState struct {
	mu         sync.RWMutex
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

var (
	_ Socket = (*reqSocket)(nil)
	_ wpool  = (*reqMWritter)(nil)
	_ rpool  = (*reqQRreader)(nil)
)
