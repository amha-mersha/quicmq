package quicmq

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
)

// Transport is the interface for pluggable network transports.
// Implementations must provide methods to dial outgoing connections
// and listen for incoming connections.
//
// QuicMQ ships with a QUIC transport registered by default.
// Additional transports (e.g. TCP, IPC) can be registered via
// RegisterTransport.
type Transport interface {
	// Dial creates a new outgoing connection to the given address.
	Dial(ctx context.Context, addr string) (net.Conn, error)

	// Listen creates a net.Listener bound to the given address.
	Listen(ctx context.Context, addr string) (net.Listener, error)
}

// UnknownTransportError records an error when trying to
// use an unknown transport.
type UnknownTransportError struct {
	Name string
}

func (ute UnknownTransportError) Error() string {
	return fmt.Sprintf("quicmq: unknown transport %q", ute.Name)
}

var _ error = (*UnknownTransportError)(nil)

// Transports returns the sorted list of currently registered transport names.
func Transports() []string {
	return drivers.names()
}

// RegisterTransport registers a new transport with quicmq.
// It returns an error if a transport with the same name is already registered.
func RegisterTransport(name string, trans Transport) error {
	return drivers.add(name, trans)
}

type transports struct {
	sync.RWMutex
	db map[string]Transport
}

func (ts *transports) get(name string) (Transport, bool) {
	ts.RLock()
	defer ts.RUnlock()
	v, ok := ts.db[name]
	return v, ok
}

func (ts *transports) add(name string, trans Transport) error {
	ts.Lock()
	defer ts.Unlock()
	if old, dup := ts.db[name]; dup {
		return fmt.Errorf("quicmq: duplicate transport %q (%T)", name, old)
	}
	ts.db[name] = trans
	return nil
}

func (ts *transports) names() []string {
	ts.RLock()
	defer ts.RUnlock()
	o := make([]string, 0, len(ts.db))
	for k := range ts.db {
		o = append(o, k)
	}
	sort.Strings(o)
	return o
}

var drivers = transports{
	db: make(map[string]Transport),
}

// splitAddr splits an endpoint string like "quic://host:port" into
// the transport name and the address.
func splitAddr(ep string) (transport, addr string, err error) {
	parts := strings.SplitN(ep, "://", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("quicmq: invalid address %q (expected scheme://addr)", ep)
	}
	return parts[0], parts[1], nil
}

// --- Context keys for passing TLS and QUIC configs through the transport layer ---

type ctxKey int

const (
	ctxKeyServerTLS ctxKey = iota
	ctxKeyClientTLS
	ctxKeyQUICConfig
	ctxKeyQlogDir
)

// withServerTLS stores a server-side TLS configuration in the context.
func withServerTLS(ctx context.Context, cfg *tls.Config) context.Context {
	if cfg == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyServerTLS, cfg)
}

// withClientTLS stores a client-side TLS configuration in the context.
func withClientTLS(ctx context.Context, cfg *tls.Config) context.Context {
	if cfg == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyClientTLS, cfg)
}

// serverTLSFromContext extracts the server TLS config from context, or nil.
func serverTLSFromContext(ctx context.Context) *tls.Config {
	v, _ := ctx.Value(ctxKeyServerTLS).(*tls.Config)
	return v
}

// clientTLSFromContext extracts the client TLS config from context, or nil.
func clientTLSFromContext(ctx context.Context) *tls.Config {
	v, _ := ctx.Value(ctxKeyClientTLS).(*tls.Config)
	return v
}

// withQlogDir stores a qlog output directory in the context.
func withQlogDir(ctx context.Context, dir string) context.Context {
	if dir == "" {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyQlogDir, dir)
}

// qlogDirFromContext extracts the qlog output directory from context, or "".
func qlogDirFromContext(ctx context.Context) string {
	v, _ := ctx.Value(ctxKeyQlogDir).(string)
	return v
}
