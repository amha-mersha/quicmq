// Demo 4 — PUB/SUB Resilience Publisher
//
// Publishes on three topics every second:
//   sensors/temp    — temperature readings (random float)
//   sensors/hum     — humidity readings
//   alert           — periodic alert messages
//
// Kill it with Ctrl+C to demonstrate subscriber reconnection, then restart
// to show automatic reconnect on the subscriber side.
//
// ── SETUP ─────────────────────────────────────────────────────────────────
// Run this on the Raspberry Pi.
//
// Build for Pi (ARM64):
//
//	GOARCH=arm64 GOOS=linux go build -o demo4_pub ./benchmarks/demos/04_pubsub_resilience/publisher
//	scp demo4_pub pi@<PI_IP>:~/
//
// ENV VARS (all optional):
//
//	LISTEN_PORT   UDP/TCP port for the PUB socket  (default: 7007)
//	TRANSPORT     "quic" or "tcp"                  (default: quic)
//	INTERVAL_MS   publish interval in milliseconds (default: 1000)
//
// FIREWALL (QUIC uses UDP, TCP uses TCP — allow both to be safe):
//	sudo ufw allow 7007/udp
//	sudo ufw allow 7007/tcp
//
// DEMO FLOW:
//  1. Start the subscriber on the laptop first (so it's ready to connect).
//  2. Start this publisher — subscriber should connect within 1-2 seconds.
//  3. Watch messages flow in the subscriber window.
//  4. Press Ctrl+C here to kill the publisher.
//     Subscriber will show reconnection attempts.
//  5. Run ./demo4_pub again to restart.
//     Subscriber reconnects and messages resume.
// ──────────────────────────────────────────────────────────────────────────

package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
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
	fmt.Printf(" \033[36m»\033[0m "+format+"\n", args...)
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func topicColor(topic string) string {
	switch {
	case strings.HasPrefix(topic, "sensors/temp"):
		return "33" // yellow
	case strings.HasPrefix(topic, "sensors/hum"):
		return "36" // cyan
	case strings.HasPrefix(topic, "alert"):
		return "31" // red
	default:
		return "37"
	}
}

func main() {
	port := envOrDefault("LISTEN_PORT", "7007")
	transport := envOrDefault("TRANSPORT", "quic")
	intervalMs, _ := strconv.Atoi(envOrDefault("INTERVAL_MS", "1000"))
	if intervalMs < 100 {
		intervalMs = 100
	}
	interval := time.Duration(intervalMs) * time.Millisecond

	banner("QuicMQ Demo 4 · PUB/SUB Publisher")
	info("Transport: %s", strings.ToUpper(transport))
	info("Endpoint:  %s://0.0.0.0:%s", transport, port)
	info("Topics:    sensors/temp  |  sensors/hum  |  alert")
	info("Interval:  %v per topic cycle", interval)
	fmt.Println()
	info("Kill with \033[1mCtrl+C\033[0m to demo subscriber reconnection.")
	info("Restart to show automatic reconnect on the subscriber.\n")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		fmt.Printf("\n \033[1;31m⚡ Publisher shutting down (Ctrl+C received)\033[0m\n")
		fmt.Printf(" \033[33m  → Switch to the subscriber window to watch reconnection.\033[0m\n")
		fmt.Printf(" \033[33m  → Restart this binary to see automatic reconnect.\033[0m\n\n")
		cancel()
	}()

	pub := quicmq.NewPub(ctx)
	endpoint := transport + "://0.0.0.0:" + port
	if err := pub.Listen(endpoint); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: listen %s: %v\n", endpoint, err)
		os.Exit(1)
	}
	info("Listening. Waiting for subscribers...\n")

	seq := 0
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	type topicMsg struct {
		topic   string
		payload func() string
	}

	topics := []topicMsg{
		{
			"sensors/temp",
			func() string {
				return fmt.Sprintf("sensors/temp  temp=%.1f°C  seq=%d",
					18.0+rand.Float64()*15.0, seq)
			},
		},
		{
			"sensors/hum",
			func() string {
				return fmt.Sprintf("sensors/hum   hum=%.0f%%     seq=%d",
					40.0+rand.Float64()*40.0, seq)
			},
		},
	}

	alertEvery := 5 // send alert every N cycles

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			seq++

			for _, t := range topics {
				payload := t.payload()
				msg := quicmq.NewMsgString(payload)
				if err := pub.Send(msg); err != nil {
					if ctx.Err() != nil {
						return
					}
					fmt.Fprintf(os.Stderr, " send error: %v\n", err)
					continue
				}
				c := topicColor(t.topic)
				fmt.Printf(" \033[%sm[PUB]\033[0m  %s\n", c, payload)
			}

			if seq%alertEvery == 0 {
				alertMsg := fmt.Sprintf("alert         SYSTEM_OK  seq=%d", seq)
				msg := quicmq.NewMsgString(alertMsg)
				if err := pub.Send(msg); err != nil {
					if ctx.Err() != nil {
						return
					}
					continue
				}
				fmt.Printf(" \033[31m[PUB]\033[0m  %s\n", alertMsg)
			}
		}
	}
}
