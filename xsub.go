// Copyright 2024 The QuicMQ Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package quicmq

import (
	"context"
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
	xsub := &xsubSocket{socket: newSocket(ctx, XSub, opts...)}
	return xsub
}

// xsubSocket is an XSUB QuicMQ socket.
type xsubSocket struct {
	*socket
}

var (
	_ Socket = (*xsubSocket)(nil)
)
