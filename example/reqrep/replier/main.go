package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"

	"quicmq"
)

func main() {
	port := flag.Int("port", 9001, "Port to listen on")
	flag.Parse()

	ctx := context.Background()

	// The replier listens for incoming requests.
	rep := quicmq.NewRep(ctx)
	defer rep.Close()

	addr := fmt.Sprintf("quic://0.0.0.0:%d", *port)
	if err := rep.Listen(addr); err != nil {
		log.Fatalf("listen: %v", err)
	}

	listenAddr := rep.Addr()
	if listenAddr != nil {
		fmt.Printf("Replier listening on %s\n", listenAddr.String())
	} else {
		fmt.Printf("Replier listening on %s\n", addr)
	}

	printLocalAddrs()

	for {
		// Receive a request.
		msg, err := rep.Recv()
		if err != nil {
			log.Fatalf("recv: %v", err)
		}

		fmt.Printf("Received request: %s\n", msg.Frames[0])

		// Send a reply.
		replyData := fmt.Sprintf("Reply to %s", string(msg.Frames[0]))
		reply := quicmq.NewMsgString(replyData)

		if err := rep.Send(reply); err != nil {
			log.Fatalf("send: %v", err)
		}
		fmt.Printf("Sent reply: %s\n", replyData)
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
