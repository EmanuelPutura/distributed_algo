package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/EmanuelPutura/distributed_algo/admin"
	"github.com/EmanuelPutura/distributed_algo/network"
	"github.com/EmanuelPutura/distributed_algo/protobuf"
)

const HUB_IP = "127.0.0.1"
const HUB_PORT = 5000
const PROC_IP = "127.0.0.1"
const PROC_PORT = 5004

const OWNER = "emanuel"
const INDEX = 1

func main() {
	var err error = admin.RegisterProcess(HUB_IP, HUB_PORT, PROC_IP, PROC_PORT, OWNER, INDEX)
	if err != nil {
		fmt.Println(err)
		return
	}

	network_messages := make(chan *protobuf.Message, 4096)

	network.TcpListen(PROC_IP, PROC_PORT, func(data []byte) {
		message, err := admin.ParseNetworkMessage(data)
		if err != nil {
			return
		}

		network_messages <- message
	})

	go func() {
		for message := range network_messages {
			fmt.Printf("Received message: %s", message)
			switch message.NetworkMessage.Message.Type {
			case protobuf.Message_PROC_DESTROY_SYSTEM:
			case protobuf.Message_PROC_INITIALIZE_SYSTEM:
			default:
			}
		}
	}()

	exit_channel := make(chan os.Signal, 1)
	signal.Notify(exit_channel, syscall.SIGINT, syscall.SIGTERM)
	<-exit_channel
}
