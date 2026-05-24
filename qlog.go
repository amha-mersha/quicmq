package quicmq

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"
)

// bufferedWriteCloser wraps a bufio.Writer over an io.Closer.
// It flushes buffered bytes before forwarding Close, matching the
// behaviour of quic-go's internal utils.BufferedWriteCloser.
type bufferedWriteCloser struct {
	*bufio.Writer
	closer io.Closer
}

func (b *bufferedWriteCloser) Close() error {
	if err := b.Writer.Flush(); err != nil {
		return err
	}
	return b.closer.Close()
}

// makeQlogTracer returns a quic.Config-compatible Tracer function that writes
// one .sqlog file per QUIC connection into dir.
//
// File names follow the same convention as DefaultConnectionTracer:
//
//	<dir>/<odcid>_client.sqlog   (dialing side)
//	<dir>/<odcid>_server.sqlog   (listening side)
//
// The directory is created automatically if it does not already exist.
func makeQlogTracer(dir string) func(context.Context, bool, quic.ConnectionID) qlogwriter.Trace {
	return func(_ context.Context, isClient bool, connID quic.ConnectionID) qlogwriter.Trace {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil
		}
		role := "server"
		if isClient {
			role = "client"
		}
		path := fmt.Sprintf("%s/%s_%s.sqlog", strings.TrimRight(dir, "/"), connID, role)
		f, err := os.Create(path)
		if err != nil {
			return nil
		}
		wc := &bufferedWriteCloser{
			Writer: bufio.NewWriter(f),
			closer: f,
		}
		fs := qlogwriter.NewConnectionFileSeq(wc, isClient, connID, []string{qlog.EventSchema})
		go fs.Run()
		return fs
	}
}
