package main

import quicmq "quicmq/lib"

func main() {
	context := quicmq.NewContext()
	socket, _ := context.NewSocket(quicmq.REP)
	socket.Bind("tcp://127.0.0.1:5000")
	socket.Bind("tcp://127.0.0.1:6000")

	for {
		msg, _ := socket.Recv(0)
		println("Got", string(msg))
		socket.Send(msg, 0)
	}
}
