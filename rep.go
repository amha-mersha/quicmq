package quicmq

import (
	"context"
	"fmt"
	"sync"
)

// NewRep returns a new REP QuicMQ socket.
// The returned socket value is initially unbound.
//
// REP sockets implement the replier side of the request-reply pattern.
// Each Recv receives a request; each subsequent Send delivers the reply
// back to the originating requester. The empty delimiter frame inserted
// by REQ is used to route the reply to the correct peer.
//
// Usage:
//
//	rep := quicmq.NewRep(ctx)
//	rep.Listen("quic://0.0.0.0:9000")
//	for {
//	    msg, _ := rep.Recv()   // receive request
//	    rep.Send(reply)        // send reply to requester
//	}
func NewRep(ctx context.Context, opts ...Option) Socket {
	rep := &repSocket{socket: newSocket(ctx, Rep, opts...)}
	sharedState := newRepState()
	rep.w = newRepWriter(rep.ctx, sharedState)
	rep.r = newRepReader(rep.ctx, sharedState)
	return rep
}

// repSocket is a REP QuicMQ socket.
type repSocket struct {
	*socket
}

// Send sends a reply to the last received request.
func (rep *repSocket) Send(msg Msg) error {
	ctx, cancel := context.WithTimeout(rep.ctx, rep.Timeout())
	defer cancel()
	return rep.w.write(ctx, msg)
}

// SendMulti sends a multipart reply to the last received request.
func (rep *repSocket) SendMulti(msg Msg) error {
	msg.multipart = true
	ctx, cancel := context.WithTimeout(rep.ctx, rep.Timeout())
	defer cancel()
	return rep.w.write(ctx, msg)
}

// Recv receives a request message.
func (rep *repSocket) Recv() (Msg, error) {
	ctx, cancel := context.WithCancel(rep.ctx)
	defer cancel()
	var msg Msg
	err := rep.r.read(ctx, &msg)
	return msg, err
}

// --- repMsg: associates a message with the connection it came from ---

type repMsg struct {
	conn *Conn
	msg  Msg
}

// --- repReader: reads requests from multiple connections ---

type repReader struct {
	ctx   context.Context
	state *repState

	mu    sync.Mutex
	conns []*Conn

	msgCh chan repMsg
}

func newRepReader(ctx context.Context, state *repState) *repReader {
	const qsize = 10
	return &repReader{
		ctx:   ctx,
		msgCh: make(chan repMsg, qsize),
		state: state,
	}
}

func (r *repReader) addConn(c *Conn) {
	r.mu.Lock()
	r.conns = append(r.conns, c)
	r.mu.Unlock()
	go r.listen(r.ctx, c)
}

func (r *repReader) rmConn(conn *Conn) {
	r.mu.Lock()
	defer r.mu.Unlock()

	cur := -1
	for i := range r.conns {
		if r.conns[i] == conn {
			cur = i
			break
		}
	}
	if cur >= 0 {
		r.conns = append(r.conns[:cur], r.conns[cur+1:]...)
	}
}

func (r *repReader) read(ctx context.Context, msg *Msg) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case rm := <-r.msgCh:
		if rm.msg.err != nil {
			return rm.msg.err
		}
		pre, innerMsg := splitReq(rm.msg)
		if pre == nil {
			return fmt.Errorf("quicmq: REP: invalid request message (no envelope delimiter)")
		}
		*msg = innerMsg
		r.state.set(rm.conn, pre)
	}
	return nil
}

func (r *repReader) listen(ctx context.Context, conn *Conn) {
	defer r.rmConn(conn)
	defer conn.Close()

	for {
		msg := conn.read()
		select {
		case <-ctx.Done():
			return
		default:
			if msg.err != nil {
				return
			}
			r.msgCh <- repMsg{conn, msg}
		}
	}
}

func (r *repReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var err error
	for _, conn := range r.conns {
		e := conn.Close()
		if e != nil && err == nil {
			err = e
		}
	}
	r.conns = nil
	return err
}

// splitReq finds the empty delimiter frame in a request envelope.
// Returns preamble (including the delimiter) and the payload.
//
// Envelope format: [identity frames...] [empty delimiter] [payload frames...]
func splitReq(envelope Msg) (preamble [][]byte, msg Msg) {
	for i, frame := range envelope.Frames {
		if len(frame) != 0 {
			continue
		}
		preamble = envelope.Frames[:i+1]
		if i+1 < len(envelope.Frames) {
			msg = NewMsgFrom(envelope.Frames[i+1:]...)
		}
		return
	}
	return nil, Msg{}
}

// --- repWriter: sends replies to the originating requester ---

type repSendPayload struct {
	conn     *Conn
	preamble [][]byte
	msg      Msg
}

func (p repSendPayload) buildReplyMsg() Msg {
	frames := make([][]byte, 0, len(p.preamble)+len(p.msg.Frames))
	frames = append(frames, p.preamble...)
	frames = append(frames, p.msg.Frames...)
	return NewMsgFrom(frames...)
}

type repWriter struct {
	ctx   context.Context
	state *repState

	mu    sync.Mutex
	conns []*Conn

	sendCh chan repSendPayload
}

func newRepWriter(ctx context.Context, state *repState) *repWriter {
	r := &repWriter{
		ctx:    ctx,
		state:  state,
		sendCh: make(chan repSendPayload),
	}
	go r.run()
	return r
}

func (r *repWriter) addConn(w *Conn) {
	r.mu.Lock()
	r.conns = append(r.conns, w)
	r.mu.Unlock()
}

func (r *repWriter) rmConn(conn *Conn) {
	r.mu.Lock()
	defer r.mu.Unlock()

	cur := -1
	for i := range r.conns {
		if r.conns[i] == conn {
			cur = i
			break
		}
	}
	if cur >= 0 {
		r.conns = append(r.conns[:cur], r.conns[cur+1:]...)
	}
}

func (r *repWriter) write(ctx context.Context, msg Msg) error {
	conn, preamble := r.state.get()
	if conn == nil {
		return fmt.Errorf("quicmq: REP: no pending request to reply to")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.ctx.Done():
		return r.ctx.Err()
	case r.sendCh <- repSendPayload{conn, preamble, msg}:
		// libzmq REP FSM: after sending the reply, the socket is back to
		// "ready to recv" state. Clear the routing state so a stray second
		// Send returns the "no pending request" error rather than blindly
		// re-sending to the same peer.
		r.state.clear()
		return nil
	}
}

func (r *repWriter) run() {
	for {
		select {
		case <-r.ctx.Done():
			return
		case payload, ok := <-r.sendCh:
			if !ok {
				return
			}
			r.sendPayload(payload)
		}
	}
}

func (r *repWriter) sendPayload(payload repSendPayload) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, conn := range r.conns {
		if conn == payload.conn {
			reply := payload.buildReplyMsg()
			_ = conn.SendMsg(reply)
			return
		}
	}
}

func (r *repWriter) Close() error {
	close(r.sendCh)
	r.mu.Lock()
	defer r.mu.Unlock()

	var err error
	for _, conn := range r.conns {
		e := conn.Close()
		if e != nil && err == nil {
			err = e
		}
	}
	r.conns = nil
	return err
}

// --- repState: tracks last requester for routing replies ---

type repState struct {
	mu       sync.Mutex
	conn     *Conn
	preamble [][]byte // includes delimiter
}

func newRepState() *repState {
	return &repState{}
}

func (r *repState) get() (conn *Conn, preamble [][]byte) {
	r.mu.Lock()
	conn = r.conn
	preamble = r.preamble
	r.mu.Unlock()
	return
}

func (r *repState) set(conn *Conn, pre [][]byte) {
	r.mu.Lock()
	r.conn = conn
	r.preamble = pre
	r.mu.Unlock()
}

// clear resets the routing state — called after a reply has been
// queued. Subsequent Send calls without a preceding Recv will hit the
// "no pending request" guard in repWriter.write.
func (r *repState) clear() {
	r.mu.Lock()
	r.conn = nil
	r.preamble = nil
	r.mu.Unlock()
}

var (
	_ Socket = (*repSocket)(nil)
	_ rpool  = (*repReader)(nil)
	_ wpool  = (*repWriter)(nil)
)
