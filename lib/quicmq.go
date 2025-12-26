package quicmq

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"math/big"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

const (
	ErrAddrAlreadyBound       string = "address is already bound"
	ErrNotConnected           string = "address has not been connected before"
	ErrSendingQueueFull       string = "sending queue is full"
	ErrInvalidOptionValue     string = "invalid option value"
	ErrSocketClosed           string = "socket is already closed"
	ErrTimeout                string = "operation timed out"
	ErrConnectionBeingClosed  string = "connection is being closed"
	ErrTopicAlreadySubscribed string = "topic is already subscribed by this socket"
	ErrTopicDoesNotExist      string = "topic does not exist"
	ErrConnectionClosed       string = "connection closed"
)

type QuicContext struct {
	sync.Mutex
	sockets        map[SocketID]Socket
	sockIDGen      SocketID
	MaxSocketSlots int
	recvBufSize    int
	sendBufSize    int
}

func NewQuicContext() (*QuicContext, error) {
	return &QuicContext{
		sockets:     make(map[SocketID]Socket),
		recvBufSize: 8 * 1024 * 1024, // 8MB default
		sendBufSize: 8 * 1024 * 1024,
	}, nil
}

func (mq *QuicContext) Close() error {
	mq.Lock()
	sockets := mq.sockets
	mq.sockets = nil
	mq.Unlock()

	for _, socket := range sockets {
		socket.Close()
	}
	return nil
}

func (mq *QuicContext) getNextSocketID() (SocketID, error) {
	mq.Lock()
	defer mq.Unlock()

	if mq.MaxSocketSlots > 0 && len(mq.sockets) >= mq.MaxSocketSlots {
		return 0, errors.New("max sockets reached")
	}

	mq.sockIDGen++
	return mq.sockIDGen, nil
}


// ParseAddr parses QUIC addresses in multiple formats:
// - "quic://host:port" (default, uses UDP)
// - "quic+udp://host:port" (explicit UDP)
// - "host:port" (inferred as quic://)
// Returns a net.UDPAddr for use with QUIC transport
func ParseAddr(addr string) (*net.UDPAddr, error) {
	var host string

	// Handle various address formats
	switch {
	case strings.HasPrefix(addr, "quic+udp://"):
		host = addr[len("quic+udp://"):]
	case strings.HasPrefix(addr, "quic://"):
		host = addr[len("quic://"):]
	case strings.HasPrefix(addr, "udp://"):
		host = addr[len("udp://"):]
	case !strings.Contains(addr, "://"):
		// No scheme, assume quic://
		host = addr
	default:
		return nil, errors.New("unsupported address scheme")
	}

	if host == "" {
		return nil, errors.New("empty address")
	}

	if !strings.ContainsRune(host, ':') {
		return nil, errors.New("missing port")
	}

	udpAddr, err := net.ResolveUDPAddr("udp", host)
	if err != nil {
		return nil, err
	}

	return udpAddr, nil
}

func (qmq *QuicContext) createUDPSocket(addr string, bufSize *int) (*net.UDPConn, error) {
	udpAddr, err := ParseAddr(addr)
	if udpAddr == nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}

	if bufSize != nil {
		if err := conn.SetReadBuffer(*bufSize); err != nil {
			conn.Close()
			return nil, err
		}
		if err := conn.SetWriteBuffer(*bufSize); err != nil {
			conn.Close()
			return nil, err
		}
	} else {
		if err := conn.SetReadBuffer(qmq.recvBufSize); err != nil {
			conn.Close()
			return nil, err
		}
		if err := conn.SetWriteBuffer(qmq.sendBufSize); err != nil {
			conn.Close()
			return nil, err
		}
	}
	return conn, nil
}

type (
	SocketID    int
	StreamID    int
	TransportID int
)

func (mq *QuicContext) NewSocket(socketType SocketType, opts ...Option) (Socket, error) {
	socketID, err := mq.getNextSocketID()
	if err != nil {
		return nil, err
	}

	var socket Socket

	switch socketType {
	case PUB:
		socket = &pubSocket{
			baseSocket: &baseSocket{
				socketID:      socketID,
				context:       mq,
				maxBufferSize: 100,
				transportConn: make(map[string]*transportConnection),
			},
			sendTimeout:       30 * time.Second,
			subscriberStreams: make(map[quic.StreamID]*quic.Stream),
		}

	case SUB:
		socket = &subSocket{
			baseSocket: &baseSocket{
				socketID:      socketID,
				context:       mq,
				maxBufferSize: 100,
				transportConn: make(map[string]*transportConnection),
			},
			recvTimeout:    30 * time.Second,
			recvQueue:      make(chan []byte, 100),
			topicToStreams: make(map[string]*quic.Stream),
		}

	default:
		return nil, errors.New("unsupported socket type")
	}

	// Apply options
	for _, opt := range opts {
		if err := opt(socket); err != nil {
			socket.Close()
			return nil, err
		}
	}

	// Store in context
	mq.Lock()
	mq.sockets[socketID] = socket
	mq.Unlock()

	return socket, nil
}

func GenerateClientTLSConfig() *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"quicmq"},
	}
}

func GenerateServerTLSConfig() *tls.Config {
	key, _ := rsa.GenerateKey(rand.Reader, 1024)
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(24 * time.Hour),
	}
	certDER, _ := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)

	return &tls.Config{
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{certDER},
			PrivateKey:  key,
		}},
		NextProtos: []string{"quicmq"},
	}
}
