// Copyright 2024 The QuicMQ Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package quicmq

import (
	"context"
	"net"
)

// NewXPub returns a new XPUB QuicMQ socket.
//
// XPUB is an extended version of PUB that exposes subscription messages
// received from downstream subscribers. Unlike PUB, both Send and Recv
// are functional: Send publishes messages (with topic filtering), while
// Recv returns the raw subscription/unsubscription commands received from
// connected SUB/XSUB peers.
//
// XPUB sockets are typically used in intermediary devices (proxies) that
// need to forward subscriptions between front-end and back-end sockets.
//
// The returned socket value is initially unbound.
func NewXPub(ctx context.Context, opts ...Option) Socket {
	xpub := &xpubSocket{sck: newSocket(ctx, XPub, opts...)}
	xpub.sck.w = newPubMWriter(xpub.sck.ctx)
	xpub.sck.r = newPubQReader(xpub.sck.ctx)
	return xpub
}

// xpubSocket is an XPUB QuicMQ socket.
type xpubSocket struct {
	sck *socket
}

// Close closes the open Socket.
func (xpub *xpubSocket) Close() error {
	return xpub.sck.Close()
}

// Send puts the message on the outbound send queue.
// Messages are distributed to all connected subscribers whose topic
// subscriptions match the message's first frame.
func (xpub *xpubSocket) Send(msg Msg) error {
	return xpub.sck.Send(msg)
}

// SendMulti puts the message on the outbound send queue as a multipart message.
func (xpub *xpubSocket) SendMulti(msg Msg) error {
	return xpub.sck.SendMulti(msg)
}

// Recv receives the next subscription command from a connected peer.
// Unlike PUB, XPUB's Recv returns subscription messages (frames starting
// with 0x01 for subscribe or 0x00 for unsubscribe). This allows the
// application to observe and react to subscription changes.
func (xpub *xpubSocket) Recv() (Msg, error) {
	return xpub.sck.Recv()
}

// Listen binds a local endpoint to the Socket.
func (xpub *xpubSocket) Listen(ep string) error {
	return xpub.sck.Listen(ep)
}

// Dial connects a remote endpoint to the Socket.
func (xpub *xpubSocket) Dial(ep string) error {
	return xpub.sck.Dial(ep)
}

// Type returns the type of this Socket.
func (xpub *xpubSocket) Type() SocketType {
	return xpub.sck.Type()
}

// Addr returns the listener's address.
// Addr returns nil if the socket isn't a listener.
func (xpub *xpubSocket) Addr() net.Addr {
	return xpub.sck.Addr()
}

// GetOption retrieves an option for a socket.
func (xpub *xpubSocket) GetOption(name string) (interface{}, error) {
	return xpub.sck.GetOption(name)
}

// SetOption sets an option for a socket.
func (xpub *xpubSocket) SetOption(name string, value interface{}) error {
	return xpub.sck.SetOption(name, value)
}

// Topics returns the sorted list of topics that connected peers are subscribed to.
func (xpub *xpubSocket) Topics() []string {
	return xpub.sck.topics()
}

var (
	_ Socket = (*xpubSocket)(nil)
	_ Topics = (*xpubSocket)(nil)
)
