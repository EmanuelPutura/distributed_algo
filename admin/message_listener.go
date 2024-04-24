package admin

import (
	"fmt"

	"github.com/EmanuelPutura/distributed_algo/network"
	"github.com/EmanuelPutura/distributed_algo/protobuf"
	"github.com/EmanuelPutura/distributed_algo/system"
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

func (message_listener *MessageListener) Start(hub_ip string, hub_port int32, owner string, index int32) {
	message_listener.listen()
	go func() {
		systems := make(map[string]*system.System)
		for {
			for message := range message_listener.channel {
				fmt.Printf("Received message: %s\n", message)
				switch message.NetworkMessage.Message.Type {
				case protobuf.Message_PROC_DESTROY_SYSTEM:
				case protobuf.Message_PROC_INITIALIZE_SYSTEM:
					system := system.Create(
						message.NetworkMessage.Message,
						hub_ip,
						hub_port,
						owner,
						index,
					).Init()

					go system.Start()
					systems[message.SystemId] = system
				default:
					if system, exists := systems[message.SystemId]; exists {
						system.Enqueue(message)
					} else {
						// TODO
						fmt.Println("Error, system does not exist!")
					}
				}
			}
		}
	}()
}
