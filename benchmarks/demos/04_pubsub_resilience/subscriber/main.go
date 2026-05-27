// Demo 4 — PUB/SUB Resilience Subscriber
//
// Opens three SUB sockets, each subscribing to a different topic:
//   sensors/temp  — temperature only
//   sensors/hum   — humidity only
//   (empty)       — everything (all topics)
//
// Shows:
//  • Topic filtering in action (each socket receives only its subscribed topic).
//  • Automatic reconnection when the publisher is killed.
//  • Exponential-backoff retry intervals while publisher is down.
//  • Resume of message delivery when publisher restarts.
//
// ── SETUP ─────────────────────────────────────────────────────────────────
// 1. Optionally start this subscriber BEFORE the publisher — it will wait
//    patiently until the publisher comes up.
// 2. Start demo4_pub on the Raspberry Pi.
// 3. Watch messages flow here.
// 4. Kill the publisher (Ctrl+C on the Pi) to see reconnect behavior.
// 5. Restart the publisher to see automatic reconnect.
//
// Build:
//   go build -o demo4_sub ./benchmarks/demos/04_pubsub_resilience/subscriber
//
// Run:
//   SERVER_ADDR=192.168.1.5 ./demo4_sub
//
// ENV VARS:
//   SERVER_ADDR   IP of the Raspberry Pi  (REQUIRED)
//   PUB_PORT      Publisher port           (default: 7007)
//   TRANSPORT     "quic" or "tcp"          (default: quic)
// ──────────────────────────────────────────────────────────────────────────

package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"quicmq"
)

const bannerW = 64

func banner(title string) {
	border := strings.Repeat("═", bannerW)
	pad := (bannerW - len(title)) / 2
	right := bannerW - pad - len(title)
	fmt.Printf("\n\033[1;34m╔%s╗\033[0m\n", border)
	fmt.Printf("\033[1;34m║\033[0m\033[1m%s%s%s\033[0m\033[1;34m║\033[0m\n",
		strings.Repeat(" ", pad), title, strings.Repeat(" ", right))
	fmt.Printf("\033[1;34m╚%s╝\033[0m\n\n", border)
}

func info(format string, args ...any) {
	fmt.Printf(" \033[36m·\033[0m "+format+"\n", args...)
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		fmt.Fprintf(os.Stderr, "\033[1;31mFATAL: env var %s is required.\033[0m\n", key)
		fmt.Fprintf(os.Stderr, "  Example: SERVER_ADDR=192.168.1.5 ./demo4_sub\n")
		os.Exit(1)
	}
	return v
}

// subConfig defines one subscriber.
type subConfig struct {
	label     string
	topic     string
	labelColor string // ANSI color code for the label
}

// mu serializes all terminal output so lines from three goroutines don't mix.
var mu sync.Mutex

func ts() string {
	return time.Now().Format("15:04:05.000")
}

// runSubscriber manages one SUB socket for its lifetime. It reconnects
// automatically (via the socket's built-in autoReconnect) and prints a clear
// message when the connection drops or resumes.
func runSubscriber(ctx context.Context, cfg subConfig, endpoint string) {
	labelStr := fmt.Sprintf("%-14s", "["+cfg.label+"]")

	printLine := func(icon, color, format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		msg := fmt.Sprintf(format, args...)
		fmt.Printf(" %s \033[%sm%s\033[0m  \033[2m%s\033[0m  %s\n",
			ts(), cfg.labelColor, labelStr, icon, msg)
	}

	// Build the SUB socket with visible reconnect intervals so evaluators can
	// see the exponential backoff in action.
	sub := quicmq.NewSub(ctx,
		quicmq.WithAutomaticReconnect(true),
		quicmq.WithReconnectInterval(500*time.Millisecond),    // base: 500 ms
		quicmq.WithReconnectIntervalMax(5*time.Second),        // max: 5 s
		quicmq.WithDialerMaxRetries(-1),                       // retry forever
		quicmq.WithTimeout(30*time.Second),
	)
	defer sub.Close()

	topicLabel := cfg.topic
	if topicLabel == "" {
		topicLabel = "(all topics)"
	}
	printLine("⏳", cfg.labelColor, "Connecting to %s  topic: \033[1m%s\033[0m", endpoint, topicLabel)

	if err := sub.Dial(endpoint); err != nil {
		printLine("✗", "31", "Dial failed: %v", err)
		return
	}

	if err := sub.SetOption(quicmq.OptionSubscribe, cfg.topic); err != nil {
		printLine("✗", "31", "Subscribe failed: %v", err)
		return
	}
	printLine("✓", cfg.labelColor, "Connected and subscribed to \033[1m%s\033[0m", topicLabel)

	received := 0
	lastOK := time.Now()
	silentTimeout := 3 * time.Second // consider publisher gone after this

	for {
		// Use a short recv context to detect when the publisher has gone quiet.
		recvCtx, cancel := context.WithTimeout(ctx, silentTimeout)
		_ = recvCtx

		// Unfortunately Socket.Recv() doesn't take a context directly — it uses
		// the socket's internal context. We detect silence by watching the gap
		// between messages instead.
		msgCh := make(chan quicmq.Msg, 1)
		errCh := make(chan error, 1)
		go func() {
			msg, err := sub.Recv()
			if err != nil {
				errCh <- err
			} else {
				msgCh <- msg
			}
		}()

		select {
		case <-ctx.Done():
			cancel()
			return

		case err := <-errCh:
			cancel()
			if ctx.Err() != nil {
				return
			}
			printLine("⚡", "31", "Connection lost (%v) — waiting for publisher to restart...", err)
			// The socket auto-reconnects internally. We give it a moment and
			// then loop back to Recv, which blocks until reconnect succeeds.
			time.Sleep(200 * time.Millisecond)
			continue

		case msg := <-msgCh:
			cancel()
			received++
			now := time.Now()
			if now.Sub(lastOK) > silentTimeout {
				printLine("✓", cfg.labelColor, "Reconnected! Messages resuming ↓")
			}
			lastOK = now

			payload := ""
			if len(msg.Frames) > 0 {
				payload = string(msg.Frames[0])
			}
			printLine("✓", cfg.labelColor, "#%04d  %s", received, payload)

		case <-time.After(silentTimeout):
			cancel()
			if ctx.Err() != nil {
				return
			}
			gap := time.Since(lastOK).Round(time.Millisecond)
			printLine("⏳", "33", "Publisher silent for %v — reconnect attempts in progress...", gap)
		}
	}
}

func main() {
	serverAddr := mustEnv("SERVER_ADDR")
	port := envOrDefault("PUB_PORT", "7007")
	transport := envOrDefault("TRANSPORT", "quic")
	endpoint := transport + "://" + serverAddr + ":" + port

	banner("QuicMQ Demo 4 · PUB/SUB: Resilience + Topic Filtering")
	info("Publisher: %s", endpoint)
	fmt.Println()
	info("Three subscribers with different topic filters:")
	info("  \033[33m[sensors/temp ]\033[0m  receives \033[1monly\033[0m temperature messages")
	info("  \033[36m[sensors/hum  ]\033[0m  receives \033[1monly\033[0m humidity messages")
	info("  \033[32m[all topics   ]\033[0m  receives \033[1meverything\033[0m")
	fmt.Println()
	info("Kill the publisher with Ctrl+C on the Pi, then restart it.")
	info("Watch reconnect attempts here, then delivery resuming.\n")

	subs := []subConfig{
		{"sensors/temp", "sensors/temp", "33"},  // yellow
		{"sensors/hum", "sensors/hum", "36"},    // cyan
		{"all topics", "", "32"},                // green
	}

	ctx := context.Background()
	var wg sync.WaitGroup
	for _, cfg := range subs {
		wg.Add(1)
		go func(c subConfig) {
			defer wg.Done()
			runSubscriber(ctx, c, endpoint)
		}(cfg)
		time.Sleep(50 * time.Millisecond) // stagger slightly for cleaner output
	}

	wg.Wait()
}
