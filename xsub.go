// Copyright 2024 The QuicMQ Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package quicmq

import (
	"context"
	"net"
)

// NewXSub returns a new XSUB QuicMQ socket.
//
// XSUB is an extended version of SUB that exposes raw subscription
// control. Unlike SUB (which uses SetOption for topic management),
// XSUB sends subscription commands directly as messages:
//
//	// Subscribe to "weather" topic:
//	xsub.Send(quicmq.NewMsg(append([]byte{0x01}, "weather"...)))
//
//	// Unsubscribe from "weather" topic:
//	xsub.Send(quicmq.NewMsg(append([]byte{0x00}, "weather"...)))
//
// XSUB sockets are typically used in intermediary devices (proxies) that
// need to forward subscriptions between front-end and back-end sockets.
//
// The returned socket value is initially unbound.
func NewXSub(ctx context.Context, opts ...Option) Socket {
	xsub := &xsubSocket{sck: newSocket(ctx, XSub, opts...)}
	return xsub
}

// xsubSocket is an XSUB QuicMQ socket.
type xsubSocket struct {
	sck *socket
}

// Close closes the open Socket.
func (xsub *xsubSocket) Close() error {
	return xsub.sck.Close()
}

// Send puts the message on the outbound send queue.
// For XSUB, Send is used to transmit raw subscription commands
// (starting with 0x01 for subscribe or 0x00 for unsubscribe).
func (xsub *xsubSocket) Send(msg Msg) error {
	return xsub.sck.Send(msg)
}

// SendMulti puts the message on the outbound send queue as a multipart message.
func (xsub *xsubSocket) SendMulti(msg Msg) error {
	return xsub.sck.SendMulti(msg)
}

// Recv receives a complete message from a connected publisher.
func (xsub *xsubSocket) Recv() (Msg, error) {
	return xsub.sck.Recv()
}

// Listen binds a local endpoint to the Socket.
func (xsub *xsubSocket) Listen(ep string) error {
	return xsub.sck.Listen(ep)
}

// Dial connects a remote endpoint to the Socket.
func (xsub *xsubSocket) Dial(ep string) error {
	return xsub.sck.Dial(ep)
}

// Type returns the type of this Socket.
func (xsub *xsubSocket) Type() SocketType {
	return xsub.sck.Type()
}

// Addr returns the listener's address.
// Addr returns nil if the socket isn't a listener.
func (xsub *xsubSocket) Addr() net.Addr {
	return xsub.sck.Addr()
}

// GetOption retrieves an option for a socket.
func (xsub *xsubSocket) GetOption(name string) (interface{}, error) {
	return xsub.sck.GetOption(name)
}

// SetOption sets an option for a socket.
func (xsub *xsubSocket) SetOption(name string, value interface{}) error {
	return xsub.sck.SetOption(name, value)
}

var (
	_ Socket = (*xsubSocket)(nil)
)
