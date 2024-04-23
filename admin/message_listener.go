package admin

import (
	"fmt"

	"github.com/EmanuelPutura/distributed_algo/network"
	"github.com/EmanuelPutura/distributed_algo/protobuf"
)

type MessageListener struct {
	ip      string
	port    int32
	channel chan *protobuf.Message
}

func Create(ip string, port int32) *MessageListener {
	return &MessageListener{
		ip:      ip,
		port:    port,
		channel: make(chan *protobuf.Message, 4096),
	}
}

func (message_listener *MessageListener) listen() {
	network.TcpListen(message_listener.ip, message_listener.port, func(data []byte) {
		message, err := ParseNetworkMessage(data)
		if err != nil {
			return
		}

		message_listener.channel <- message
	})
}

func (message_listener *MessageListener) Start() {
	message_listener.listen()
	go func() {
		for message := range message_listener.channel {
			fmt.Printf("Received message: %s\n", message)
			switch message.NetworkMessage.Message.Type {
			case protobuf.Message_PROC_DESTROY_SYSTEM:
			case protobuf.Message_PROC_INITIALIZE_SYSTEM:
			default:
			}
		}
	}()
}
