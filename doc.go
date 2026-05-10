// Package quicmq implements ZeroMQ-style messaging patterns over QUIC transport.
//
// # Overview
//
// QuicMQ provides broker-less pub/sub messaging with built-in encryption via
// QUIC (RFC 9000). The API follows the go-zeromq/zmq4 style: sockets are
// created with simple constructors and no explicit context or broker object is
// required.
//
// # Encryption
//
// QuicMQ relies on QUIC's mandatory TLS 1.3 encryption — every connection is
// encrypted by default. There is no separate ZMTP encryption layer. For
// development, GenerateTLSConfig and InsecureClientTLSConfig produce
// self-signed credentials. For production use, load real certificates via
// NewTLSConfig and NewClientTLSConfig, or supply your own *tls.Config
// through the WithListenTLS and WithDialTLS options.
//
// # Socket Types
//
// The following socket types are available:
//
//   - PUB: Publishes messages to connected subscribers. Messages are
//     distributed only to subscribers whose topic subscriptions match
//     the message's first frame (topic prefix matching). PUB sockets
//     cannot receive application messages.
//
//   - SUB: Subscribes to topics and receives matching messages from a
//     connected publisher. Topics are managed via SetOption:
//
//     sub.SetOption(quicmq.OptionSubscribe, "weather")
//     sub.SetOption(quicmq.OptionUnsubscribe, "weather")
//
//   - XPUB: Extended publisher that exposes subscription commands received
//     from connected subscribers. Unlike PUB, Recv() returns subscription
//     change messages. Useful for building proxy/broker devices.
//
//   - XSUB: Extended subscriber that sends subscription commands as raw
//     messages instead of using SetOption. Subscriptions are controlled by
//     sending frames starting with 0x01 (subscribe) or 0x00 (unsubscribe)
//     followed by the topic string. Useful for building proxy/broker devices.
//
// # Wire Format
//
// Messages are framed using a simple length-prefixed binary protocol:
//
//	[1 byte flags] [4 byte big-endian payload length] [payload]
//
// The flags byte uses bit 0 as a "has-more" indicator for multipart messages.
// This format is intentionally simpler than ZMTP because both ends of a
// QuicMQ connection run the same library — there is no need for
// interoperability with C libzmq.
//
// # Transport Architecture
//
// QuicMQ uses a pluggable transport system. The QUIC transport is
// registered by default during package initialization. Additional
// transports (TCP, IPC, etc.) can be added via RegisterTransport:
//
//	quicmq.RegisterTransport("tcp", myTCPTransport{})
//
// Endpoint addresses use a URI scheme to select the transport:
//
//	pub.Listen("quic://0.0.0.0:9000")   // QUIC transport
//	sub.Dial("quic://127.0.0.1:9000")   // QUIC transport
//
// # Quick Start
//
// Publisher:
//
//	pub := quicmq.NewPub(context.Background())
//	defer pub.Close()
//	pub.Listen("quic://0.0.0.0:9000")
//	pub.Send(quicmq.NewMsgString("weather temperature=25°C"))
//
// Subscriber:
//
//	sub := quicmq.NewSub(context.Background())
//	defer sub.Close()
//	sub.Dial("quic://127.0.0.1:9000")
//	sub.SetOption(quicmq.OptionSubscribe, "weather")
//	msg, err := sub.Recv()
package quicmq
