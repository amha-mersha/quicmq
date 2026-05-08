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

	// The requester connects to the replier.
	req := quicmq.NewReq(ctx)
	defer req.Close()

	if err := req.Dial("quic://127.0.0.1:9001"); err != nil {
		log.Fatalf("dial: %v", err)
	}
	fmt.Println("Requester connected to quic://127.0.0.1:9001")

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
