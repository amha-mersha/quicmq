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
	port := flag.Int("port", 9000, "Port to listen on")
	flag.Parse()

	ctx := context.Background()

	pub := quicmq.NewPub(ctx)
	defer pub.Close()

	addr := fmt.Sprintf("quic://0.0.0.0:%d", *port)
	if err := pub.Listen(addr); err != nil {
		log.Fatalf("listen: %v", err)
	}
	
	listenAddr := pub.Addr()
	if listenAddr != nil {
		fmt.Printf("Publisher listening on %s (local port: %s)\n", listenAddr.String(), listenAddr.String())
	} else {
		fmt.Printf("Publisher listening on %s\n", addr)
	}

	printLocalAddrs()

	i := 0
	for {
		topic := "weather"
		data := fmt.Sprintf("%s Temperature is %d°C", topic, 20+i%10)
		msg := quicmq.NewMsgString(data)

		if err := pub.Send(msg); err != nil {
			log.Printf("send: %v", err)
		} else {
			fmt.Printf("Published: %s\n", data)
		}

		i++
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
