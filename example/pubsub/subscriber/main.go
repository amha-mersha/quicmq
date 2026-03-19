package main

import (
	"context"
	"fmt"
	"log"

	"quicmq"
)

func main() {
	ctx := context.Background()

	sub := quicmq.NewSub(ctx)
	defer sub.Close()

	if err := sub.Dial("quic://127.0.0.1:9000"); err != nil {
		log.Fatalf("dial: %v", err)
	}
	fmt.Println("Subscriber connected to quic://127.0.0.1:9000")

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
