package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"quicmq"
)

func main() {
	ctx := context.Background()

	pub := quicmq.NewPub(ctx)
	defer pub.Close()

	if err := pub.Listen("quic://127.0.0.1:9000"); err != nil {
		log.Fatalf("listen: %v", err)
	}
	fmt.Println("Publisher listening on quic://127.0.0.1:9000")

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
