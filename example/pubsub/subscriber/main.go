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
	addr := flag.String("addr", "quic://127.0.0.1:9000", "Address to connect to")
	flag.Parse()
	printLocalAddrs()

	ctx := context.Background()

	sub := quicmq.NewSub(ctx)
	defer sub.Close()

	if err := sub.Dial(*addr); err != nil {
		log.Fatalf("dial: %v", err)
	}
	fmt.Printf("Subscriber connected to %s\n", *addr)

	// Subscribe to the "weather" topic.
	if err := sub.SetOption(quicmq.OptionSubscribe, "weather"); err != nil {
		log.Fatalf("subscribe: %v", err)
	}
	fmt.Println("Subscribed to topic: weather")

	for {
		msg, err := sub.Recv()
		if err != nil {
			log.Fatalf("recv: %v", err)
		}
		fmt.Printf("Received: %s\n", msg.Frames[0])
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
