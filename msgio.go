package quicmq

import (
	"context"
	"io"
	"sync"
)

// rpool is the interface that reads messages from a pool of connections.
type rpool interface {
	io.Closer
	addConn(r *Conn)
	rmConn(r *Conn)
	read(ctx context.Context, msg *Msg) error
}

// wpool is the interface that writes messages to a pool of connections.
type wpool interface {
	io.Closer
	addConn(w *Conn)
	rmConn(r *Conn)
	write(ctx context.Context, msg Msg) error
}

// --- qreader: queued-message reader (from zmq4) ---

type qreader struct {
	ctx context.Context
	mu  sync.RWMutex
	rs  []*Conn
	c   chan Msg
	sem *semaphore
}

func newQReader(ctx context.Context) *qreader {
	const qrsize = 10
	return &qreader{
		ctx: ctx,
		c:   make(chan Msg, qrsize),
		sem: newSemaphore(),
	}
}

func (q *qreader) Close() error {
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

func (q *qreader) addConn(r *Conn) {
	q.mu.Lock()
	q.sem.enable()
	q.rs = append(q.rs, r)
	q.mu.Unlock()
	go q.listen(q.ctx, r)
}

func (q *qreader) rmConn(r *Conn) {
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

func (q *qreader) read(ctx context.Context, msg *Msg) error {
	q.sem.lock(ctx)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case *msg = <-q.c:
	}
	return msg.err
}

func (q *qreader) listen(ctx context.Context, r *Conn) {
	defer q.rmConn(r)
	defer r.Close()

	for {
		msg := r.read()
		select {
		case <-ctx.Done():
			return
		default:
			q.c <- msg
			if msg.err != nil {
				return
			}
		}
	}
}

// --- mwriter: multi-connection writer (from zmq4) ---

type mwriter struct {
	ctx context.Context
	mu  sync.Mutex
	ws  []*Conn
	sem *semaphore
}

func newMWriter(ctx context.Context) *mwriter {
	return &mwriter{
		ctx: ctx,
		sem: newSemaphore(),
	}
}

func (w *mwriter) Close() error {
	w.mu.Lock()
	var err error
	for _, ww := range w.ws {
		e := ww.Close()
		if e != nil && err == nil {
			err = e
		}
	}
	w.ws = nil
	w.mu.Unlock()
	return err
}

func (mw *mwriter) addConn(w *Conn) {
	mw.mu.Lock()
	mw.sem.enable()
	mw.ws = append(mw.ws, w)
	mw.mu.Unlock()
}

func (mw *mwriter) rmConn(w *Conn) {
	mw.mu.Lock()
	defer mw.mu.Unlock()

	cur := -1
	for i := range mw.ws {
		if mw.ws[i] == w {
			cur = i
			break
		}
	}
	if cur >= 0 {
		mw.ws = append(mw.ws[:cur], mw.ws[cur+1:]...)
	}
}

func (w *mwriter) write(ctx context.Context, msg Msg) error {
	w.sem.lock(ctx)
	w.mu.Lock()
	defer w.mu.Unlock()

	var firstErr error
	for _, ww := range w.ws {
		if err := ww.SendMsg(msg); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// --- semaphore: readiness gate ---

type semaphore struct {
	ready chan struct{}
}

func newSemaphore() *semaphore {
	return &semaphore{ready: make(chan struct{})}
}

func (sem *semaphore) enable() {
	select {
	case _, ok := <-sem.ready:
		if ok {
			close(sem.ready)
		}
	default:
		close(sem.ready)
	}
}

func (sem *semaphore) lock(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-sem.ready:
	}
}

var (
	_ rpool = (*qreader)(nil)
	_ wpool = (*mwriter)(nil)
)
