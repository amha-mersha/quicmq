package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"time"

	"quicmq"
)

func main() {
	addr := flag.String("addr", "quic://127.0.0.1:9001", "Address of the replier to connect to")
	connectTimeout := flag.Duration("connect-timeout", 30*time.Second, "How long to keep retrying before giving up on the initial connection")
	retry := flag.Duration("retry", 250*time.Millisecond, "Delay between connection attempts")
	count := flag.Int("count", 5, "Number of requests to send")
	flag.Parse()
	printLocalAddrs()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := quicmq.NewReq(ctx,
		quicmq.WithDialerRetry(*retry),
		quicmq.WithDialerMaxRetries(-1),
		quicmq.WithDialTimeout(*connectTimeout),
		// REQ sockets shouldn't silently re-dial because the FSM would
		// drop in-flight requests; we keep the option disabled so failures
		// surface to the caller.
		quicmq.WithAutomaticReconnect(false),
	)
	defer req.Close()

	fmt.Printf("Connecting to %s (timeout=%s)...\n", *addr, *connectTimeout)
	if err := req.Dial(*addr); err != nil {
		log.Fatalf("dial: %v", err)
	}
	fmt.Printf("Requester connected to %s\n", *addr)

	for i := 0; i < *count; i++ {
		reqData := fmt.Sprintf("Request %d", i+1)
		msg := quicmq.NewMsgString(reqData)

		if err := req.Send(msg); err != nil {
			log.Fatalf("send %d: %v", i+1, err)
		}
		fmt.Printf("Sent: %s\n", reqData)

		reply, err := req.Recv()
		if err != nil {
			log.Fatalf("recv %d: %v", i+1, err)
		}

		fmt.Printf("Received: %s\n", string(reply.Frames[0]))

		time.Sleep(1 * time.Second)
	}
}

func printLocalAddrs() {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return
	}
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				fmt.Printf("Local IP: %s\n", ipnet.IP.String())
			}
		}
	}
}
