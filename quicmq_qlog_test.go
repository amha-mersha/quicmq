package quicmq

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestQlogViaEnvVar verifies that qlog .sqlog files are produced when the
// QLOGDIR environment variable is set — the standard quic-go integration path.
func TestQlogViaEnvVar(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("QLOGDIR", dir)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pub := NewPub(ctx)
	defer pub.Close()
	if err := pub.Listen("quic://127.0.0.1:0"); err != nil {
		t.Fatalf("pub.Listen: %v", err)
	}
	endpoint := fmt.Sprintf("quic://%s", pub.Addr().String())

	sub := NewSub(ctx)
	defer sub.Close()
	if err := sub.Dial(endpoint); err != nil {
		t.Fatalf("sub.Dial: %v", err)
	}
	if err := sub.SetOption(OptionSubscribe, "ping"); err != nil {
		t.Fatalf("sub.SetOption: %v", err)
	}
	time.Sleep(150 * time.Millisecond)

	if err := pub.Send(NewMsgString("ping hello")); err != nil {
		t.Fatalf("pub.Send: %v", err)
	}
	if _, err := sub.Recv(); err != nil {
		t.Fatalf("sub.Recv: %v", err)
	}

	// Give the qlog goroutines a moment to flush buffered events.
	time.Sleep(100 * time.Millisecond)
	pub.Close()
	sub.Close()
	time.Sleep(100 * time.Millisecond)

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("no .sqlog files found — qlog tracing did not activate")
	}
	for _, e := range entries {
		t.Logf("qlog file: %s (%d bytes)", e.Name(), fileSize(t, filepath.Join(dir, e.Name())))
		if !strings.HasSuffix(e.Name(), ".sqlog") {
			t.Errorf("unexpected file %q in qlog dir", e.Name())
		}
	}
	t.Logf("QLOGDIR produced %d .sqlog file(s)", len(entries))
}

// TestQlogViaOption verifies that the WithQlogDir socket option writes .sqlog
// files to the caller-specified directory, independent of the QLOGDIR env var.
func TestQlogViaOption(t *testing.T) {
	dir := t.TempDir()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pub := NewPub(ctx, WithQlogDir(dir))
	defer pub.Close()
	if err := pub.Listen("quic://127.0.0.1:0"); err != nil {
		t.Fatalf("pub.Listen: %v", err)
	}
	endpoint := fmt.Sprintf("quic://%s", pub.Addr().String())

	sub := NewSub(ctx, WithQlogDir(dir))
	defer sub.Close()
	if err := sub.Dial(endpoint); err != nil {
		t.Fatalf("sub.Dial: %v", err)
	}
	if err := sub.SetOption(OptionSubscribe, "data"); err != nil {
		t.Fatalf("sub.SetOption: %v", err)
	}
	time.Sleep(150 * time.Millisecond)

	if err := pub.Send(NewMsgString("data value=42")); err != nil {
		t.Fatalf("pub.Send: %v", err)
	}
	if _, err := sub.Recv(); err != nil {
		t.Fatalf("sub.Recv: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	pub.Close()
	sub.Close()
	time.Sleep(100 * time.Millisecond)

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("no .sqlog files found — WithQlogDir did not activate tracing")
	}

	var clientFiles, serverFiles int
	for _, e := range entries {
		name := e.Name()
		size := fileSize(t, filepath.Join(dir, name))
		t.Logf("qlog file: %s (%d bytes)", name, size)
		if size == 0 {
			t.Errorf("qlog file %q is empty", name)
		}
		if strings.HasSuffix(name, "_client.sqlog") {
			clientFiles++
		} else if strings.HasSuffix(name, "_server.sqlog") {
			serverFiles++
		}
	}
	if clientFiles == 0 {
		t.Error("no client-side .sqlog file produced")
	}
	if serverFiles == 0 {
		t.Error("no server-side .sqlog file produced")
	}
	t.Logf("WithQlogDir produced %d client + %d server .sqlog file(s)", clientFiles, serverFiles)
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Errorf("stat %q: %v", path, err)
		return 0
	}
	return info.Size()
}
