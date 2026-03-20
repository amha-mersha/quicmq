// Copyright 2024 The QuicMQ Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package quicmq

import (
	"context"
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
	xpub := &xpubSocket{socket: newSocket(ctx, XPub, opts...)}
	xpub.w = newPubMWriter(xpub.ctx)
	xpub.r = newPubQReader(xpub.ctx)
	return xpub
}

// xpubSocket is an XPUB QuicMQ socket.
type xpubSocket struct {
	*socket
}

// Topics returns the sorted list of topics that connected peers are subscribed to.
func (xpub *xpubSocket) Topics() []string {
	return xpub.topics()
}

var (
	_ Socket = (*xpubSocket)(nil)
	_ Topics = (*xpubSocket)(nil)
)
