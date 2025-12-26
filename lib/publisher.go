package quicmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

type pubSocket struct {
	*baseSocket
	lazyPublish       sync.Once
	sendTimeout       time.Duration
	subscriberStreams map[quic.StreamID]*quic.Stream
	writeQueue        chan []byte
}

func (ps *pubSocket) Bind(addr string) error {
	if ps.closed.Load() {
		return errors.New(ErrSocketClosed)
	}

	parsed, err := ParseAddr(addr)
	if err != nil {
		return err
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	addrStr := parsed.String()
	if _, exists := ps.transportConn[addrStr]; exists {
		return errors.New(ErrAddrAlreadyBound)
	}

	// Create transport and start listening
	conn, err := ps.context.createUDPSocket(addr, nil)
	if err != nil {
		return err
	}
	tr := &quic.Transport{
		Conn: conn,
	}
	listener, err := tr.Listen(
		GenerateServerTLSConfig(),
		&quic.Config{},
	)
	if err != nil {
		return err
	}

	go func() {
		for {
			quicConn, err := listener.Accept(context.Background())
			if err != nil {
				return
			}
			ps.transportConn[addrStr].mu.Lock()
			ps.transportConn[addrStr].conn = quicConn
			ps.transportConn[addrStr].mu.Unlock()

			go func() {
				for {
					stream, err := quicConn.AcceptStream(context.Background())
					if err != nil {
						return
					}
					ps.mu.Lock()
					ps.subscriberStreams[stream.StreamID()] = stream
					ps.mu.Unlock()
				}
			}()
		}
	}()

	if ps.transportConn[addrStr] == nil {
		ps.transportConn[addrStr] = &transportConnection{}
	}
	ps.transportConn[addrStr].transport = tr
	return nil
}

func (ps *pubSocket) Unbind(addr string) error {
	if ps.closed.Load() {
		return errors.New(ErrSocketClosed)
	}

	parsed, err := ParseAddr(addr)
	if err != nil {
		return err
	}

	addrStr := parsed.String()

	ps.mu.Lock()
	defer ps.mu.Unlock()

	if transportConn, exists := ps.transportConn[addrStr]; exists {
		err := transportConn.transport.Close()
		if err != nil {
			return err
		}
		quicConn := transportConn.conn
		err = quicConn.CloseWithError(quic.ApplicationErrorCode(quic.ApplicationErrorErrorCode), ErrConnectionBeingClosed)
		if err != nil {
			return err
		}
		delete(ps.transportConn, addrStr)
	}
	return nil
}

func (ps *pubSocket) Send(msg []byte) error {
	if ps.closed.Load() {
		return errors.New(ErrSocketClosed)
	}

	if len(msg) == 0 {
		return nil
	}

	ps.mu.Lock()
	if ps.writeQueue == nil {
		ps.writeQueue = make(chan []byte, 100)
	}
	select {
	case ps.writeQueue <- msg:
	default:
		return errors.New(ErrSendingQueueFull)
	}
	ps.mu.Unlock()
	ps.lazyPublish.Do(
		func() {
			go ps.handlePublishing()
		},
	)
	return nil
}

func (ps *pubSocket) handlePublishing() {
	for msg := range ps.writeQueue {
		ps.mu.Lock()
		if len(ps.subscriberStreams) == 0 {
			ps.mu.Unlock()
			continue
		}
		for _, stream := range ps.subscriberStreams {
			_, err := stream.Write(msg)
			if err != nil {
				if !errors.Is(err, &quic.StreamError{}) {
					delete(ps.subscriberStreams, stream.StreamID())
				}
			}
		}
		ps.mu.Unlock()
	}
}

func (ps *pubSocket) SendMultipart(parts [][]byte) error {
	// Not implemented yet
	// for _, part := range parts {
	// 	if err := ps.Send(part); err != nil {
	// 		return err
	// 	}
	// }
	return nil
}

func (ps *pubSocket) Recv() ([]byte, error) {
	if ps.closed.Load() {
		return nil, errors.New(ErrSocketClosed)
	}
	return nil, errors.New("publishers don't receive")
}

func (ps *pubSocket) RecvMultipart() ([][]byte, error) {
	if ps.closed.Load() {
		return nil, errors.New(ErrSocketClosed)
	}
	return nil, errors.New("publishers don't receive")
}

func (ps *pubSocket) Connect(addr string) error {
	return errors.New("publishers don't connect")
}

func (ps *pubSocket) Disconnect(addr string) error {
	if ps.closed.Load() {
		return errors.New(ErrSocketClosed)
	}
	return errors.New("publishers don't disconnect")
}

func (ps *pubSocket) Subscribe(topic string) error {
	if ps.closed.Load() {
		return errors.New(ErrSocketClosed)
	}
	return errors.New("publishers don't subscribe")
}

func (ps *pubSocket) Unsubscribe(topic string) error {
	return errors.New("publishers don't unsubscribe")
}

func (ps *pubSocket) SetOption(opt SocketOption, value any) error {
	if ps.closed.Load() {
		return errors.New(ErrSocketClosed)
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()

	switch opt {
	case OptionSendTimeout:
		if d, ok := value.(time.Duration); ok {
			ps.sendTimeout = d
			return nil
		} else {
			return errors.New(ErrInvalidOptionValue)
		}
	case OptionSendBuffer:
		if size, ok := value.(int); ok {
			if size <= 0 {
				return fmt.Errorf("%s : buffer size must be greater than 0", ErrInvalidOptionValue)
			}
			ps.maxBufferSize = size
			return nil
		} else {
			return errors.New(ErrInvalidOptionValue)
		}
	case OptionLinger:
		if d, ok := value.(time.Duration); ok {
			ps.sendTimeout = d
			return nil
		} else {
			return errors.New(ErrInvalidOptionValue)
		}
	}
	return errors.New(ErrInvalidOptionValue)
}

func (ps *pubSocket) GetOption(opt SocketOption) (any, error) {
	if ps.closed.Load() {
		return nil, errors.New(ErrSocketClosed)
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()

	switch opt {
	case OptionSendTimeout:
		return ps.sendTimeout, nil
	case OptionSendBuffer:
		return ps.maxBufferSize, nil
	case OptionLinger:
		return ps.sendTimeout, nil
	}
	return nil, errors.New(ErrInvalidOptionValue)
}

func (ps *pubSocket) Close() error {
	if !ps.closed.CompareAndSwap(false, true) {
		return errors.New(ErrSocketClosed)
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	for _, stream := range ps.subscriberStreams {
		if err := stream.Close(); err != nil {
			return err
		}
	}

	for _, transportConn := range ps.transportConn {
		// lock per transportConn while we close its resources
		transportConn.mu.Lock()

		if transportConn.conn != nil {
			if err := transportConn.conn.CloseWithError(0, ErrConnectionClosed); err != nil {
				transportConn.mu.Unlock()
				return err
			}
		}

		if transportConn.transport != nil {
			if err := transportConn.transport.Close(); err != nil {
				transportConn.mu.Unlock()
				return err
			}
		}

		transportConn.mu.Unlock()
	}

	delete(ps.context.sockets, ps.socketID)
	return nil
}
