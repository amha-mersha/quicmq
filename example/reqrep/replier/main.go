package main

import (
	"context"
	"fmt"
	"log"

	"quicmq"
)

func main() {
	ctx := context.Background()

	// The replier listens for incoming requests.
	rep := quicmq.NewRep(ctx)
	defer rep.Close()

	if err := rep.Listen("quic://127.0.0.1:9001"); err != nil {
		log.Fatalf("listen: %v", err)
	}
	fmt.Println("Replier listening on quic://127.0.0.1:9001")

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
