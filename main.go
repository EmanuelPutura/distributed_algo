package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/EmanuelPutura/distributed_algo/admin"
)

func main() {
	hub_ip := flag.String("hub-ip", "127.0.0.1", "The hub IP")
	hub_port := flag.Int("hub-port", 5000, "The hub port")
	proc_ip := flag.String("proc-ip", "127.0.0.1", "The current process IP")
	proc_port := flag.Int("proc-port", 5004, "The current process port")
	owner := flag.String("owner", "emanuel", "The owner of the process")
	index := flag.Int("index", 1, "The index of the process")

	flag.Parse()

	var err error = admin.RegisterProcess(*hub_ip, int32(*hub_port), *proc_ip, int32(*proc_port), *owner, int32(*index))
	if err != nil {
		fmt.Println(err)
		return
	}

	var message_listener *admin.MessageListener = admin.Create(*proc_ip, int32(*proc_port))
	message_listener.Start(*hub_ip, int32(*hub_port), *owner, int32(*index))

	exit_channel := make(chan os.Signal, 1)
	signal.Notify(exit_channel, syscall.SIGINT, syscall.SIGTERM)
	<-exit_channel
}
