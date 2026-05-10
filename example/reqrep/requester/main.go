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
	addr := flag.String("addr", "quic://127.0.0.1:9001", "Address of the replier to remote connect")
	flag.Parse()
	printLocalAddrs()

	ctx := context.Background()

	// The requester connects to the replier.
	req := quicmq.NewReq(ctx)
	defer req.Close()

	if err := req.Dial(*addr); err != nil {
		log.Fatalf("dial: %v", err)
	}
	fmt.Printf("Requester connected to %s\n", *addr)

	for i := range 5 {
		// Send a request.
		reqData := fmt.Sprintf("Request %d", i+1)
		msg := quicmq.NewMsgString(reqData)

		if err := req.Send(msg); err != nil {
			log.Fatalf("send: %v", err)
		}
		fmt.Printf("Sent: %s\n", reqData)

		// Wait for the reply.
		reply, err := req.Recv()
		if err != nil {
			log.Fatalf("recv: %v", err)
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
