package quicmq

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
)

var ErrClosedConn = errors.New("quicmq: read/write on closed connection")

// Conn wraps a net.Conn with length-prefixed framing and topic subscription tracking.
type Conn struct {
	typ    SocketType
	rw     net.Conn
	Server bool

	mu     sync.RWMutex
	topics map[string]struct{} // set of subscribed topics

	closed         int32
	onCloseErrorCB func(c *Conn)
}

// Open creates a new Conn over the given net.Conn.
// An optional onCloseErrorCB is called when the connection encounters an error.
func Open(rw net.Conn, sockType SocketType, server bool, onCloseErrorCB func(c *Conn)) (*Conn, error) {
	if rw == nil {
		return nil, fmt.Errorf("quicmq: invalid nil connection")
	}

	conn := &Conn{
		typ:            sockType,
		rw:             rw,
		Server:         server,
		topics:         make(map[string]struct{}),
		onCloseErrorCB: onCloseErrorCB,
	}

	return conn, nil
}

// Close closes the underlying connection.
func (c *Conn) Close() error {
	return c.rw.Close()
}

// SendMsg sends a message over the wire using length-prefixed framing.
// Wire format per frame:
//
//	[1 byte flags] [4 byte big-endian length] [payload]
//
// Flags: 0x01 = has-more (multi-frame message).
func (c *Conn) SendMsg(msg Msg) error {
	if c.Closed() {
		return ErrClosedConn
	}

	nframes := len(msg.Frames)
	for i, frame := range msg.Frames {
		var flag byte
		if i < nframes-1 {
			flag |= 0x01 // has-more
		}

		// Write flag
		if _, err := c.rw.Write([]byte{flag}); err != nil {
			c.checkIO(err)
			return fmt.Errorf("quicmq: send flag: %w", err)
		}

		// Write length (4 bytes, big-endian)
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(frame)))
		if _, err := c.rw.Write(lenBuf[:]); err != nil {
			c.checkIO(err)
			return fmt.Errorf("quicmq: send length: %w", err)
		}

		// Write payload
		if _, err := c.rw.Write(frame); err != nil {
			c.checkIO(err)
			return fmt.Errorf("quicmq: send frame %d/%d: %w", i+1, nframes, err)
		}
	}
	return nil
}

// read reads a complete message (potentially multi-frame) from the wire.
func (c *Conn) read() Msg {
	var msg Msg
	hasMore := true

	for hasMore {
		// Read flag byte
		var flagBuf [1]byte
		_, msg.err = io.ReadFull(c.rw, flagBuf[:])
		if msg.err != nil {
			c.checkIO(msg.err)
			return msg
		}
		hasMore = (flagBuf[0] & 0x01) != 0

		// Read length (4 bytes)
		var lenBuf [4]byte
		_, msg.err = io.ReadFull(c.rw, lenBuf[:])
		if msg.err != nil {
			c.checkIO(msg.err)
			return msg
		}
		size := binary.BigEndian.Uint32(lenBuf[:])

		// Read payload
		body := make([]byte, size)
		_, msg.err = io.ReadFull(c.rw, body)
		if msg.err != nil {
			c.checkIO(msg.err)
			return msg
		}

		msg.Frames = append(msg.Frames, body)
	}

	return msg
}

// subscribe processes a subscription command message.
// Frame format: [0x00 or 0x01][topic...]
//   - 0x00 = unsubscribe
//   - 0x01 = subscribe
func (c *Conn) subscribe(msg Msg) {
	c.mu.Lock()
	v := msg.Frames[0]
	k := string(v[1:])
	switch v[0] {
	case 0:
		delete(c.topics, k)
	case 1:
		c.topics[k] = struct{}{}
	}
	c.mu.Unlock()
}

// subscribed checks if a given topic matches any subscription on this connection.
func (c *Conn) subscribed(topic string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for k := range c.topics {
		switch {
		case k == "":
			// subscribed to everything
			return true
		case strings.HasPrefix(topic, k):
			return true
		}
	}
	return false
}

// SetClosed marks the connection as closed and triggers the callback.
func (c *Conn) SetClosed() {
	if wasClosed := atomic.CompareAndSwapInt32(&c.closed, 0, 1); wasClosed {
		c.notifyOnCloseError()
	}
}

// Closed returns whether the connection has been marked as closed.
func (c *Conn) Closed() bool {
	return atomic.LoadInt32(&c.closed) == 1
}

// checkIO inspects an I/O error and, if it indicates the connection is
// no longer usable, marks the Conn closed and fires onCloseErrorCB so
// the owning socket can schedule reconnection (libzmq's reconnect_ivl
// behaviour).
//
// We don't set read/write deadlines on streamConn, so any error here is
// terminal — including QUIC's idle-timeout (net.Error with Timeout=true),
// which previously slipped through. Without marking the conn closed,
// auto-reconnect never fires and SUB/REQ sockets hang waiting on a dead
// QUIC stream.
func (c *Conn) checkIO(err error) {
	if err == nil {
		return
	}
	c.SetClosed()
}

func (c *Conn) notifyOnCloseError() {
	if c.onCloseErrorCB == nil {
		return
	}
	c.onCloseErrorCB(c)
}
