package quicmq

// zmtp.go implements ZMTP 3.1 (NULL security mechanism) framing and handshake.
//
// Wire formats
// ─────────────
// Short frame (body ≤ 255 bytes):
//   [flags : 1 byte][size : 1 byte][body : size bytes]
//
// Long frame  (body > 255 bytes):
//   [flags|0x02 : 1 byte][size : 8 bytes BE][body : size bytes]
//
// Flag bits:
//   0x01 = MORE  – more frames follow in this message
//   0x02 = LONG  – 8-byte length field (set automatically)
//   0x04 = CMD   – command frame (READY, SUBSCRIBE, etc.)
//
// Greeting:
//   64 bytes, both peers send simultaneously on connection.
//
// Handshake:
//   Each peer sends READY command (CMD frame) carrying Socket-Type property.

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
)

// zmtpMarker is implemented by net.Conn values returned by both the TCP and
// QUIC transports. Open() uses it to trigger the ZMTP 3.1 handshake and use
// ZMTP wire framing on the connection, making both transports protocol-identical
// so benchmark comparisons isolate transport behaviour only.
type zmtpMarker interface {
	isZMTPConn()
}

// ─── Greeting ───────────────────────────────────────────────────────────────

const zmtpGreetingSize = 64

// nullMechanism is the 20-byte security mechanism field for NULL.
var nullMechanism = [20]byte{'N', 'U', 'L', 'L'}

// zmtpBuildGreeting writes a 64-byte ZMTP 3.1 greeting into dst.
func zmtpBuildGreeting(dst []byte, asServer bool) {
	// Signature
	dst[0] = 0xFF
	dst[8] = 0x01 // padding; marks ZMTP ≥ 3 to the peer
	dst[9] = 0x7F
	// Version
	dst[10] = 3 // major
	dst[11] = 1 // minor
	// Security mechanism: "NULL" + zero padding to 20 bytes
	copy(dst[12:32], nullMechanism[:])
	// as-server flag
	if asServer {
		dst[32] = 0x01
	}
	// bytes 33-63: filler zeros (already zero from make)
}

// ─── Handshake ──────────────────────────────────────────────────────────────

// zmtpHandshake performs the ZMTP 3.1 NULL mechanism handshake on rw:
//  1. Both peers send their 64-byte greeting simultaneously.
//  2. Each peer sends a READY command carrying its Socket-Type.
//  3. Each peer reads and validates the peer's READY command.
func zmtpHandshake(rw net.Conn, sockType SocketType, server bool) error {
	// Build and send our greeting.
	greeting := make([]byte, zmtpGreetingSize)
	zmtpBuildGreeting(greeting, server)
	if _, err := rw.Write(greeting); err != nil {
		return fmt.Errorf("zmtp: send greeting: %w", err)
	}

	// Read peer greeting.
	var peerGreeting [zmtpGreetingSize]byte
	if _, err := io.ReadFull(rw, peerGreeting[:]); err != nil {
		return fmt.Errorf("zmtp: read greeting: %w", err)
	}

	// Validate signature.
	if peerGreeting[0] != 0xFF || peerGreeting[9] != 0x7F {
		return fmt.Errorf("zmtp: invalid greeting signature")
	}
	if peerGreeting[10] != 3 {
		return fmt.Errorf("zmtp: unsupported peer major version %d", peerGreeting[10])
	}

	// Send our READY command then read the peer's.
	if err := zmtpSendReady(rw, sockType); err != nil {
		return fmt.Errorf("zmtp: send READY: %w", err)
	}
	if err := zmtpRecvReady(rw); err != nil {
		return fmt.Errorf("zmtp: recv READY: %w", err)
	}
	return nil
}

// zmtpSendReady sends a READY command containing the Socket-Type property.
func zmtpSendReady(w io.Writer, sockType SocketType) error {
	// Body: "\x05READY" + encoded Socket-Type property
	body := make([]byte, 0, 32)
	body = append(body, "\x05READY"...)
	body = append(body, zmtpEncodeProperty("Socket-Type", string(sockType))...)
	return zmtpWriteFrame(w, 0x04, body) // 0x04 = CMD flag
}

// zmtpRecvReady reads and validates a READY command from the peer.
func zmtpRecvReady(r io.Reader) error {
	flags, body, err := zmtpReadRawFrame(r)
	if err != nil {
		return err
	}
	if flags&0x04 == 0 {
		return fmt.Errorf("zmtp: expected CMD frame, got flags=0x%02x", flags)
	}
	if !strings.HasPrefix(string(body), "\x05READY") {
		n := len(body)
		if n > 8 {
			n = 8
		}
		return fmt.Errorf("zmtp: expected READY command, got %q", body[:n])
	}
	return nil
}

// zmtpEncodeProperty encodes one ZMTP metadata property:
//   [name-len : 1 byte][name][value-len : 4 bytes BE][value]
func zmtpEncodeProperty(name, value string) []byte {
	b := make([]byte, 1+len(name)+4+len(value))
	b[0] = byte(len(name))
	copy(b[1:], name)
	binary.BigEndian.PutUint32(b[1+len(name):], uint32(len(value)))
	copy(b[1+len(name)+4:], value)
	return b
}

// ─── Frame read / write ──────────────────────────────────────────────────────

// zmtpWriteFrame writes a single ZMTP frame to w.
// flags is the base flag byte; the LONG bit (0x02) is set automatically when
// len(body) > 255.
func zmtpWriteFrame(w io.Writer, flags byte, body []byte) error {
	if len(body) > 255 {
		hdr := make([]byte, 9)
		hdr[0] = flags | 0x02
		binary.BigEndian.PutUint64(hdr[1:], uint64(len(body)))
		if _, err := w.Write(hdr); err != nil {
			return err
		}
	} else {
		if _, err := w.Write([]byte{flags, byte(len(body))}); err != nil {
			return err
		}
	}
	_, err := w.Write(body)
	return err
}

// zmtpReadRawFrame reads one ZMTP frame from r and returns the flag byte and body.
func zmtpReadRawFrame(r io.Reader) (flags byte, body []byte, err error) {
	var flagBuf [1]byte
	if _, err = io.ReadFull(r, flagBuf[:]); err != nil {
		return
	}
	flags = flagBuf[0]

	var size uint64
	if flags&0x02 != 0 {
		// Long frame: 8-byte big-endian length.
		var lenBuf [8]byte
		if _, err = io.ReadFull(r, lenBuf[:]); err != nil {
			return
		}
		size = binary.BigEndian.Uint64(lenBuf[:])
	} else {
		// Short frame: 1-byte length.
		var szBuf [1]byte
		if _, err = io.ReadFull(r, szBuf[:]); err != nil {
			return
		}
		size = uint64(szBuf[0])
	}

	body = make([]byte, size)
	_, err = io.ReadFull(r, body)
	return
}

// ─── Message-level send / recv ───────────────────────────────────────────────

// zmtpSendMsg encodes msg using ZMTP framing and writes it to w.
// Each frame becomes one ZMTP message frame; the MORE flag is set on all but
// the last frame.
func zmtpSendMsg(w io.Writer, msg Msg) error {
	n := len(msg.Frames)
	for i, frame := range msg.Frames {
		var flags byte
		if i < n-1 {
			flags |= 0x01 // MORE
		}
		if err := zmtpWriteFrame(w, flags, frame); err != nil {
			return fmt.Errorf("zmtp: write frame %d/%d: %w", i+1, n, err)
		}
	}
	return nil
}

// zmtpReadMsg reads a complete ZMTP message (all frames until MORE=0) from r.
// Command frames (CMD bit set) are silently skipped so PING/PONG etc. are
// transparent to the caller.
func zmtpReadMsg(r io.Reader) Msg {
	var msg Msg
	for {
		flags, body, err := zmtpReadRawFrame(r)
		if err != nil {
			msg.err = err
			return msg
		}
		if flags&0x04 != 0 {
			// Command frame (e.g. PING) — skip, keep reading.
			continue
		}
		msg.Frames = append(msg.Frames, body)
		if flags&0x01 == 0 {
			// Last frame of message.
			return msg
		}
	}
}
