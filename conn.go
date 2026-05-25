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
	typ     SocketType
	rw      net.Conn
	Server  bool
	useZMTP bool          // true when the connection uses ZMTP 3.1 framing
	curve   *curveSession // non-nil when CURVE encryption is active over ZMTP

	mu     sync.RWMutex
	topics map[string]struct{} // set of subscribed topics

	closed         int32
	onCloseErrorCB func(c *Conn)
}

// Open creates a new Conn over the given net.Conn.
// An optional onCloseErrorCB is called when the connection encounters an error.
//
// Handshake selection (checked in order):
//
//  1. rw implements curveTCPMarker → ZMTP 3.1 CURVE handshake + per-message encryption.
//  2. rw implements zmtpMarker     → ZMTP 3.1 NULL handshake (no encryption).
//  3. Neither                      → QUIC internal framing, no ZMTP.
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

	// curveTCPTimingDirer is implemented by tcpCURVEConn when WithCurveTimingDir
	// has been set.  Open checks for it to obtain the timing directory.
	type curveTCPTimingDirer interface{ curveHandshakeTimingDir() string }

	switch ctm := rw.(type) {
	case curveTCPMarker:
		// CURVE takes priority: it also sends a ZMTP greeting but with
		// mechanism="CURVE" and runs the full CURVE command exchange.
		timingDir := ""
		if td, ok := rw.(curveTCPTimingDirer); ok {
			timingDir = td.curveHandshakeTimingDir()
		}
		session, err := zmtpCURVEHandshake(rw, sockType, server, ctm, timingDir)
		if err != nil {
			rw.Close()
			return nil, fmt.Errorf("quicmq: curve handshake: %w", err)
		}
		conn.curve = session
		conn.useZMTP = true

	case zmtpMarker:
		// NULL mechanism: greeting + READY exchange, no encryption.
		if err := zmtpHandshake(rw, sockType, server); err != nil {
			rw.Close()
			return nil, fmt.Errorf("quicmq: zmtp handshake: %w", err)
		}
		conn.useZMTP = true
	}

	return conn, nil
}

// Close closes the underlying connection.
func (c *Conn) Close() error {
	return c.rw.Close()
}

// SendMsg sends a message over the wire.
//
//   - CURVE (TCP + CURVE): each frame is an encrypted ZMTP MESSAGE command.
//   - NULL (TCP plain):    ZMTP 3.1 short/long frames.
//   - QUIC internal:       [flag:1][len:4 BE][payload] per frame.
func (c *Conn) SendMsg(msg Msg) error {
	if c.Closed() {
		return ErrClosedConn
	}

	if c.curve != nil {
		if err := zmtpCURVESendMsg(c.rw, msg, c.curve); err != nil {
			c.checkIO(err)
			return err
		}
		return nil
	}

	if c.useZMTP {
		if err := zmtpSendMsg(c.rw, msg); err != nil {
			c.checkIO(err)
			return err
		}
		return nil
	}

	// Internal quicmq framing: [flag:1][len:4 BE][payload].
	nframes := len(msg.Frames)
	for i, frame := range msg.Frames {
		var flag byte
		if i < nframes-1 {
			flag |= 0x01 // has-more
		}
		if _, err := c.rw.Write([]byte{flag}); err != nil {
			c.checkIO(err)
			return fmt.Errorf("quicmq: send flag: %w", err)
		}
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(frame)))
		if _, err := c.rw.Write(lenBuf[:]); err != nil {
			c.checkIO(err)
			return fmt.Errorf("quicmq: send length: %w", err)
		}
		if _, err := c.rw.Write(frame); err != nil {
			c.checkIO(err)
			return fmt.Errorf("quicmq: send frame %d/%d: %w", i+1, nframes, err)
		}
	}
	return nil
}

// read reads a complete message (potentially multi-frame) from the wire.
func (c *Conn) read() Msg {
	if c.curve != nil {
		msg := zmtpCURVEReadMsg(c.rw, c.curve)
		if msg.err != nil {
			c.checkIO(msg.err)
		}
		return msg
	}

	if c.useZMTP {
		msg := zmtpReadMsg(c.rw)
		if msg.err != nil {
			c.checkIO(msg.err)
		}
		return msg
	}

	// Internal quicmq framing.
	var msg Msg
	hasMore := true
	for hasMore {
		var flagBuf [1]byte
		_, msg.err = io.ReadFull(c.rw, flagBuf[:])
		if msg.err != nil {
			c.checkIO(msg.err)
			return msg
		}
		hasMore = (flagBuf[0] & 0x01) != 0

		var lenBuf [4]byte
		_, msg.err = io.ReadFull(c.rw, lenBuf[:])
		if msg.err != nil {
			c.checkIO(msg.err)
			return msg
		}
		size := binary.BigEndian.Uint32(lenBuf[:])

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
