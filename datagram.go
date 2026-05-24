package quicmq

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

// datagramServerQUICConfig returns a server QUIC config with datagram support.
func datagramServerQUICConfig() *quic.Config {
	cfg := defaultServerQUICConfig() // Allow0RTT = true
	cfg.EnableDatagrams = true
	return cfg
}

// datagramClientQUICConfig returns a client QUIC config with datagram support.
func datagramClientQUICConfig() *quic.Config {
	cfg := defaultQUICConfig()
	cfg.EnableDatagrams = true
	return cfg
}

// serializeMsgDatagram packs a Msg into a single byte slice using the same
// per-frame wire format as stream-based sockets: [flag][4-byte len][payload].
func serializeMsgDatagram(msg Msg) []byte {
	var buf bytes.Buffer
	n := len(msg.Frames)
	for i, frame := range msg.Frames {
		var flag byte
		if i < n-1 {
			flag = 0x01
		}
		buf.WriteByte(flag)
		var lb [4]byte
		binary.BigEndian.PutUint32(lb[:], uint32(len(frame)))
		buf.Write(lb[:])
		buf.Write(frame)
	}
	return buf.Bytes()
}

// deserializeMsgDatagram parses a datagram byte slice back into a Msg.
func deserializeMsgDatagram(data []byte) (Msg, error) {
	r := bytes.NewReader(data)
	var msg Msg
	hasMore := true
	for hasMore {
		var hdr [5]byte // [flag][4-byte len]
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return msg, err
		}
		hasMore = (hdr[0] & 0x01) != 0
		size := binary.BigEndian.Uint32(hdr[1:])
		frame := make([]byte, size)
		if _, err := io.ReadFull(r, frame); err != nil {
			return msg, err
		}
		msg.Frames = append(msg.Frames, frame)
	}
	return msg, nil
}

// writeSubCmd writes one subscription command to w using the quicmq wire format.
// subscribe=true → [0x01][topic], subscribe=false → [0x00][topic].
func writeSubCmd(w io.Writer, subscribe bool, topic string) error {
	payload := make([]byte, 1+len(topic))
	if subscribe {
		payload[0] = 0x01
	}
	copy(payload[1:], topic)

	// Wire frame: [flag=0x00 (last frame)][4-byte length][payload]
	frame := make([]byte, 1+4+len(payload))
	frame[0] = 0x00
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(payload)))
	copy(frame[5:], payload)

	_, err := w.Write(frame)
	return err
}

// readSubCmd reads one subscription command from r.
func readSubCmd(r io.Reader) (subscribe bool, topic string, err error) {
	var hdr [5]byte
	if _, err = io.ReadFull(r, hdr[:]); err != nil {
		return
	}
	size := binary.BigEndian.Uint32(hdr[1:])
	payload := make([]byte, size)
	if _, err = io.ReadFull(r, payload); err != nil {
		return
	}
	if len(payload) == 0 {
		err = fmt.Errorf("quicmq: empty subscription command")
		return
	}
	subscribe = payload[0] == 0x01
	topic = string(payload[1:])
	return
}

// ─── dgpeer ─────────────────────────────────────────────────────────────────

// dgpeer tracks one connected DatagramSub: its QUIC connection and subscribed topics.
type dgpeer struct {
	qconn  *quic.Conn
	mu     sync.RWMutex
	topics map[string]struct{}
}

func (p *dgpeer) subscribed(topic string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for k := range p.topics {
		if k == "" || strings.HasPrefix(topic, k) {
			return true
		}
	}
	return false
}

// ─── DatagramPubSocket ───────────────────────────────────────────────────────

// DatagramPubSocket publishes messages over QUIC RFC 9221 unreliable datagrams.
//
// Unlike the stream-based PUB socket, messages are not retransmitted on loss,
// making this socket ideal for high-rate sensor data where a late packet is
// worse than a lost one.
//
// Subscription commands from connected DatagramSub peers travel over a
// dedicated reliable QUIC stream on the same connection — demonstrating
// QUIC's ability to multiplex reliable (control) and unreliable (data)
// channels over a single UDP flow.
type DatagramPubSocket struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.RWMutex
	peerList []*dgpeer

	ql      *quic.Listener
	tr      *quic.Transport
	udpConn *net.UDPConn
	addr    net.Addr
	props   map[string]any

	log *log.Logger
}

// NewDatagramPub creates a new DatagramPub socket.
func NewDatagramPub(ctx context.Context, opts ...Option) *DatagramPubSocket {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx2, cancel := context.WithCancel(ctx)
	s := &DatagramPubSocket{
		ctx:    ctx2,
		cancel: cancel,
		props:  make(map[string]any),
		log:    log.New(os.Stderr, "quicmq: ", 0),
	}
	// Use temporary socket to apply options (logger, TLS).
	tmp := newSocket(ctx, DatagramPub, opts...)
	s.log = tmp.log
	tmp.cancel()
	return s
}

func (s *DatagramPubSocket) Listen(ep string) error {
	_, addr, err := splitAddr(ep)
	if err != nil {
		return err
	}
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return fmt.Errorf("quicmq: datagram pub resolve %q: %w", ep, err)
	}
	s.udpConn, err = net.ListenUDP("udp", udpAddr)
	if err != nil {
		return fmt.Errorf("quicmq: datagram pub udp listen %q: %w", ep, err)
	}
	s.tr = &quic.Transport{Conn: s.udpConn}
	s.ql, err = s.tr.Listen(GenerateTLSConfig(), datagramServerQUICConfig())
	if err != nil {
		s.udpConn.Close()
		return fmt.Errorf("quicmq: datagram pub quic listen: %w", err)
	}
	s.addr = s.udpConn.LocalAddr()
	go s.acceptLoop()
	return nil
}

func (s *DatagramPubSocket) acceptLoop() {
	for {
		qconn, err := s.ql.Accept(s.ctx)
		if err != nil {
			return
		}
		peer := &dgpeer{qconn: qconn, topics: make(map[string]struct{})}
		s.mu.Lock()
		s.peerList = append(s.peerList, peer)
		s.mu.Unlock()
		go s.handlePeer(peer)
	}
}

// handlePeer reads subscription commands from the peer's control stream and
// removes the peer when the stream closes.
func (s *DatagramPubSocket) handlePeer(peer *dgpeer) {
	defer func() {
		s.mu.Lock()
		for i, p := range s.peerList {
			if p == peer {
				s.peerList = append(s.peerList[:i], s.peerList[i+1:]...)
				break
			}
		}
		s.mu.Unlock()
	}()

	stream, err := peer.qconn.AcceptStream(s.ctx)
	if err != nil {
		return
	}
	defer stream.Close()

	for {
		sub, topic, err := readSubCmd(stream)
		if err != nil {
			return
		}
		peer.mu.Lock()
		if sub {
			peer.topics[topic] = struct{}{}
		} else {
			delete(peer.topics, topic)
		}
		peer.mu.Unlock()
	}
}

// Send broadcasts msg as a QUIC datagram to all subscribers whose topic
// subscriptions match the first frame of the message.
func (s *DatagramPubSocket) Send(msg Msg) error {
	if len(msg.Frames) == 0 {
		return nil
	}
	topic := string(msg.Frames[0])
	data := serializeMsgDatagram(msg)

	s.mu.RLock()
	peers := make([]*dgpeer, len(s.peerList))
	copy(peers, s.peerList)
	s.mu.RUnlock()

	var firstErr error
	var dead []*dgpeer

	for _, peer := range peers {
		if !peer.subscribed(topic) {
			continue
		}
		if !peer.qconn.ConnectionState().SupportsDatagrams {
			s.log.Printf("peer %s does not support datagrams", peer.qconn.RemoteAddr())
			continue
		}
		if err := peer.qconn.SendDatagram(data); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			dead = append(dead, peer)
		}
	}

	if len(dead) > 0 {
		s.mu.Lock()
		for _, d := range dead {
			for i, p := range s.peerList {
				if p == d {
					s.peerList = append(s.peerList[:i], s.peerList[i+1:]...)
					break
				}
			}
		}
		s.mu.Unlock()
	}
	return firstErr
}

func (s *DatagramPubSocket) SendMulti(msg Msg) error { msg.multipart = true; return s.Send(msg) }
func (s *DatagramPubSocket) Recv() (Msg, error) {
	return Msg{}, fmt.Errorf("quicmq: DatagramPub cannot Recv")
}
func (s *DatagramPubSocket) Dial(ep string) error { return fmt.Errorf("quicmq: DatagramPub cannot Dial") }
func (s *DatagramPubSocket) Type() SocketType     { return DatagramPub }
func (s *DatagramPubSocket) Addr() net.Addr       { return s.addr }
func (s *DatagramPubSocket) GetOption(name string) (any, error) {
	v, ok := s.props[name]
	if !ok {
		return nil, ErrBadProperty
	}
	return v, nil
}
func (s *DatagramPubSocket) SetOption(name string, value any) error {
	s.props[name] = value
	return nil
}
func (s *DatagramPubSocket) Close() error {
	s.cancel()
	if s.ql != nil {
		s.ql.Close()
	}
	if s.udpConn != nil {
		s.udpConn.Close()
	}
	return nil
}

var _ Socket = (*DatagramPubSocket)(nil)

// ─── DatagramSubSocket ───────────────────────────────────────────────────────

// DatagramSubSocket receives messages published by a DatagramPub socket via
// QUIC unreliable datagrams.  Subscription filters are sent to the publisher
// over a reliable QUIC stream on the same connection.
type DatagramSubSocket struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu           sync.RWMutex
	topics       map[string]struct{}
	qconn        *quic.Conn
	ctrlStream   *quic.Stream
	tr           *quic.Transport
	udpConn      *net.UDPConn
	timeout      time.Duration
	props        map[string]any

	log *log.Logger
}

// NewDatagramSub creates a new DatagramSub socket.
func NewDatagramSub(ctx context.Context, opts ...Option) *DatagramSubSocket {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx2, cancel := context.WithCancel(ctx)
	s := &DatagramSubSocket{
		ctx:     ctx2,
		cancel:  cancel,
		topics:  make(map[string]struct{}),
		timeout: defaultTimeout,
		props:   make(map[string]any),
		log:     log.New(os.Stderr, "quicmq: ", 0),
	}
	tmp := newSocket(ctx, DatagramSub, opts...)
	s.log = tmp.log
	s.timeout = tmp.timeout
	tmp.cancel()
	return s
}

func (s *DatagramSubSocket) Dial(ep string) error {
	_, addr, err := splitAddr(ep)
	if err != nil {
		return err
	}
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return fmt.Errorf("quicmq: datagram sub resolve %q: %w", ep, err)
	}
	s.udpConn, err = net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		return fmt.Errorf("quicmq: datagram sub udp: %w", err)
	}
	s.tr = &quic.Transport{Conn: s.udpConn}

	tlsCfg := InsecureClientTLSConfig()
	qconn, err := s.tr.DialEarly(s.ctx, udpAddr, tlsCfg, datagramClientQUICConfig())
	if err != nil {
		s.udpConn.Close()
		return fmt.Errorf("quicmq: datagram sub dial %q: %w", ep, err)
	}

	// Wait for handshake so datagram support is confirmed.
	select {
	case <-qconn.HandshakeComplete():
	case <-s.ctx.Done():
		qconn.CloseWithError(0, "cancelled")
		s.udpConn.Close()
		return s.ctx.Err()
	}

	if !qconn.ConnectionState().SupportsDatagrams {
		qconn.CloseWithError(0, "no datagram support")
		s.udpConn.Close()
		return fmt.Errorf("quicmq: server at %q does not support QUIC datagrams", ep)
	}

	// Open the control stream used for subscription commands.
	stream, err := qconn.OpenStreamSync(s.ctx)
	if err != nil {
		qconn.CloseWithError(0, "control stream open failed")
		s.udpConn.Close()
		return fmt.Errorf("quicmq: datagram sub control stream: %w", err)
	}

	s.qconn = qconn
	s.ctrlStream = stream

	// Re-send any topics that were subscribed before Dial.
	s.mu.RLock()
	for topic := range s.topics {
		_ = writeSubCmd(s.ctrlStream, true, topic)
	}
	s.mu.RUnlock()

	return nil
}

// SetOption handles OptionSubscribe and OptionUnsubscribe.
func (s *DatagramSubSocket) SetOption(name string, value any) error {
	switch name {
	case OptionSubscribe:
		topic := value.(string)
		s.mu.Lock()
		s.topics[topic] = struct{}{}
		s.mu.Unlock()
		if s.ctrlStream != nil {
			return writeSubCmd(s.ctrlStream, true, topic)
		}
		return nil

	case OptionUnsubscribe:
		topic := value.(string)
		s.mu.Lock()
		delete(s.topics, topic)
		s.mu.Unlock()
		if s.ctrlStream != nil {
			return writeSubCmd(s.ctrlStream, false, topic)
		}
		return nil
	}
	s.props[name] = value
	return nil
}

// Recv blocks until a datagram arrives from the publisher that matches at
// least one subscribed topic, or until the socket context is cancelled.
func (s *DatagramSubSocket) Recv() (Msg, error) {
	if s.qconn == nil {
		return Msg{}, fmt.Errorf("quicmq: DatagramSub not connected")
	}

	ctx, cancel := context.WithTimeout(s.ctx, s.timeout)
	defer cancel()

	for {
		data, err := s.qconn.ReceiveDatagram(ctx)
		if err != nil {
			return Msg{}, err
		}
		msg, err := deserializeMsgDatagram(data)
		if err != nil {
			continue // malformed datagram — skip
		}
		if s.matchesTopic(msg) {
			return msg, nil
		}
	}
}

// matchesTopic returns true if msg's first frame has a prefix that matches
// any of the socket's subscribed topics (empty topic = subscribe to all).
func (s *DatagramSubSocket) matchesTopic(msg Msg) bool {
	if len(msg.Frames) == 0 {
		return false
	}
	topic := string(msg.Frames[0])
	s.mu.RLock()
	defer s.mu.RUnlock()
	for k := range s.topics {
		if k == "" || strings.HasPrefix(topic, k) {
			return true
		}
	}
	return false
}

// Topics returns the sorted list of subscribed topics.
func (s *DatagramSubSocket) Topics() []string {
	s.mu.RLock()
	out := make([]string, 0, len(s.topics))
	for t := range s.topics {
		out = append(out, t)
	}
	s.mu.RUnlock()
	sort.Strings(out)
	return out
}

func (s *DatagramSubSocket) Send(msg Msg) error { return fmt.Errorf("quicmq: DatagramSub cannot Send") }
func (s *DatagramSubSocket) SendMulti(msg Msg) error {
	return fmt.Errorf("quicmq: DatagramSub cannot Send")
}
func (s *DatagramSubSocket) Listen(ep string) error {
	return fmt.Errorf("quicmq: DatagramSub cannot Listen")
}
func (s *DatagramSubSocket) Type() SocketType { return DatagramSub }
func (s *DatagramSubSocket) Addr() net.Addr   { return nil }
func (s *DatagramSubSocket) GetOption(name string) (any, error) {
	v, ok := s.props[name]
	if !ok {
		return nil, ErrBadProperty
	}
	return v, nil
}
func (s *DatagramSubSocket) Close() error {
	s.cancel()
	if s.ctrlStream != nil {
		s.ctrlStream.Close()
	}
	if s.qconn != nil {
		s.qconn.CloseWithError(0, "socket closed")
	}
	if s.udpConn != nil {
		s.udpConn.Close()
	}
	return nil
}

var _ Socket = (*DatagramSubSocket)(nil)
var _ Topics = (*DatagramSubSocket)(nil)
