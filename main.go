package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/EmanuelPutura/distributed_algo/admin"
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

	var message_listener *admin.MessageListener = admin.Create(PROC_IP, PROC_PORT)
	message_listener.Start(HUB_IP, HUB_PORT, OWNER, INDEX)

	exit_channel := make(chan os.Signal, 1)
	signal.Notify(exit_channel, syscall.SIGINT, syscall.SIGTERM)
	<-exit_channel
}
