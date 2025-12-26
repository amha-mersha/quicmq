package quicmq

import (
	"context"
	"errors"
	"net"
	"strings"
	"time"

	"github.com/quic-go/quic-go"
)

type subSocket struct {
	*baseSocket
	recvQueue      chan []byte
	recvTimeout    time.Duration
	topicToStreams map[string]*quic.Stream
}

func (ss *subSocket) Send(msg []byte) error {
	if ss.closed.Load() {
		return errors.New(ErrSocketClosed)
	}
	return errors.New("subscribers don't send")
}

func (ss *subSocket) SendMultipart(parts [][]byte) error {
	if ss.closed.Load() {
		return errors.New(ErrSocketClosed)
	}
	return errors.New("subscribers don't send")
}

func (ss *subSocket) Recv() ([]byte, error) {
	if ss.closed.Load() {
		return nil, errors.New(ErrSocketClosed)
	}
	select {
	case msg := <-ss.recvQueue:
		return msg, nil
	case <-time.After(ss.recvTimeout):
		return nil, errors.New(ErrTimeout)
	}
}

func (ss *subSocket) RecvMultipart() ([][]byte, error) {
	if ss.closed.Load() {
		return nil, errors.New(ErrSocketClosed)
	}
	return nil, errors.New("not implemented yet")
}

func (ss *subSocket) Bind(addr string) error {
	if ss.closed.Load() {
		return errors.New(ErrSocketClosed)
	}
	return errors.New("subscribers don't bind")
}

func (ss *subSocket) Connect(addr string) error {
	if ss.closed.Load() {
		return errors.New(ErrSocketClosed)
	}

	ss.mu.Lock()
	defer ss.mu.Unlock()

	if _, ok := ss.transportConn[addr]; ok {
		return errors.New(ErrAddrAlreadyBound)
	}

	tlsConf := GenerateClientTLSConfig()
	remoteAddr, err := ParseAddr(addr) // Publisher's address
	if err != nil {
		return err
	}

	// Bind to ANY available local port (0.0.0.0:0)
	localAddr := &net.UDPAddr{IP: net.IPv4zero, Port: 0}
	udpConn, err := net.ListenUDP("udp", localAddr)
	if err != nil {
		return err
	}

	transport := quic.Transport{
		Conn: udpConn,
	}

	// Dial to REMOTE address
	quicConn, err := transport.Dial(context.Background(), remoteAddr, tlsConf, nil)
	if err != nil {
		udpConn.Close()
		return err
	}

	transConn := &transportConnection{
		transport: &transport,
		conn:      quicConn,
	}
	ss.transportConn[addr] = transConn

	return nil
}

func (ss *subSocket) handleIncomingMessages(stream *quic.Stream) {
	buf := make([]byte, ss.maxBufferSize)
	for {
		if ss.closed.Load() {
			return
		}
		ss.mu.Lock()
		defer ss.mu.Unlock()
		n, err := stream.Read(buf)
		if err != nil {
			return
		}
		msg := make([]byte, n)
		copy(msg, buf[:n])

		topicMsg := strings.SplitN(string(msg), ":", 2)
		if len(topicMsg) != 2 {
			continue
		}
		topic := topicMsg[0]

		_, ok := ss.topicToStreams[topic]

		if ok {
			select {
			case ss.recvQueue <- msg:
			default:
				// Drop if queue is full
			}
		}
	}
}

func (ss *subSocket) Disconnect(addr string) error {
	if ss.closed.Load() {
		return errors.New(ErrSocketClosed)
	}
	parsed, err := ParseAddr(addr)
	if err != nil {
		return err
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	addrStr := parsed.String()
	if transConn, ok := ss.transportConn[addrStr]; ok {
		transConn.mu.Lock()
		defer transConn.mu.Unlock()
		if err := transConn.transport.Close(); err != nil {
			return err
		}
		if err := transConn.conn.CloseWithError(quic.ApplicationErrorCode(quic.ApplicationErrorErrorCode), ErrConnectionBeingClosed); err != nil {
			return err
		}
		delete(ss.transportConn, addrStr)
	} else {
		return errors.New(ErrNotConnected)
	}
	return nil
}

func (ss *subSocket) Unbind(addr string) error {
	if ss.closed.Load() {
		return errors.New(ErrSocketClosed)
	}
	return errors.New("subscribers don't unbind")
}

func (ss *subSocket) Subscribe(topic string) error {
	if ss.closed.Load() {
		return errors.New(ErrSocketClosed)
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if _, ok := ss.topicToStreams[topic]; ok {
		return errors.New(ErrTopicAlreadySubscribed)
	} else {
		if len(ss.transportConn) == 0 {
			return errors.New(ErrNotConnected)
		}
		for _, tranConn := range ss.transportConn {
			err := func() error {
				tranConn.mu.Lock()
				defer tranConn.mu.Unlock()
				quicConn := tranConn.conn
				stream, err := quicConn.OpenStreamSync(context.Background()) // probably need timeout context #TODO
				if err != nil {
					return err
				}
				ss.topicToStreams[topic] = stream
				_, err = stream.Write([]byte(topic + ":ADD_ME")) // Send topic subscription prefix
				if err != nil {
					delete(ss.topicToStreams, topic)
					return err
				}
				go ss.handleIncomingMessages(stream)
				return nil
			}()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (ss *subSocket) Unsubscribe(topic string) error {
	if ss.closed.Load() {
		return errors.New(ErrSocketClosed)
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()

	if stream, ok := ss.topicToStreams[topic]; ok {
		if err := stream.Close(); err != nil {
			return err
		}
		delete(ss.topicToStreams, topic)
	} else {
		return errors.New(ErrTopicDoesNotExist)
	}
	return nil
}

func (ss *subSocket) SetOption(opt SocketOption, value any) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	switch opt {
	case OptionRecvTimeout:
		if d, ok := value.(time.Duration); ok {
			ss.recvTimeout = d
			return nil
		}
	case OptionRecvBuffer:
		if size, ok := value.(int); ok {
			ss.maxBufferSize = size
			return nil
		}
	case OptionLinger:
		if d, ok := value.(time.Duration); ok {
			ss.recvTimeout = d
			return nil
		}
	}
	return errors.New("invalid option")
}

func (ss *subSocket) GetOption(opt SocketOption) (any, error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	switch opt {
	case OptionRecvTimeout:
		return ss.recvTimeout, nil
	case OptionRecvBuffer:
		return ss.maxBufferSize, nil
	case OptionLinger:
		return ss.recvTimeout, nil
	}
	return nil, errors.New("invalid option")
}

func (ss *subSocket) Close() error {
	if !ss.closed.CompareAndSwap(false, true) {
		return errors.New(ErrSocketClosed)
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	for _, stream := range ss.topicToStreams {
		if err := stream.Close(); err != nil {
			return err
		}
	}

	for _, transportConn := range ss.transportConn {
		transportConn.mu.Lock()
		defer transportConn.mu.Unlock()
		if err := transportConn.conn.CloseWithError(0, ErrConnectionClosed); err != nil {
			return err
		}
		if err := transportConn.transport.Close(); err != nil {
			return err
		}
	}
	delete(ss.context.sockets, ss.socketID)
	return nil
}

func (ss *subSocket) Context() context.Context {
	return context.Background()
}
