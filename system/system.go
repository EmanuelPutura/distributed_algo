package system

import (
	"errors"
	"fmt"

	"github.com/EmanuelPutura/distributed_algo/app"
	perfectlink "github.com/EmanuelPutura/distributed_algo/perfect_link"
	"github.com/EmanuelPutura/distributed_algo/protobuf"
)

type System struct {
	id              string
	hub_ip          string
	hub_port        int32
	messages_queue  chan *protobuf.Message
	parent_process  *protobuf.ProcessId
	child_processes []*protobuf.ProcessId
	abstractions    map[string]Abstraction
}

func Create(message *protobuf.Message, hub_ip string, hub_port int32, owner string, index int32) *System {
	var parent_process *protobuf.ProcessId
	for _, process := range message.ProcInitializeSystem.Processes {
		if process.Owner == owner && process.Index == index {
			parent_process = process
			break
		}
	}

	return &System{
		id:              message.SystemId,
		hub_ip:          hub_ip,
		hub_port:        hub_port,
		messages_queue:  make(chan *protobuf.Message, 4096),
		parent_process:  parent_process,
		child_processes: message.ProcInitializeSystem.Processes,
		abstractions:    make(map[string]Abstraction),
	}
}

func (system *System) Destroy() {
	for _, abstraction := range system.abstractions {
		abstraction.Destroy()
	}

	close(system.messages_queue)
}

func (system *System) Enqueue(message *protobuf.Message) {
	system.messages_queue <- message
}

type AbstractionWithName struct {
	name        string
	abstraction Abstraction
}

func (system *System) createInitialAbstractions() []AbstractionWithName {
	var link *perfectlink.PerfectLink = perfectlink.Create(
		system.parent_process.Host,
		system.parent_process.Port,
		system.hub_ip,
		system.hub_port,
	).SetSystemId(system.id).SetSystemMessagesQueue(system.messages_queue).SetSystemProcesses(system.child_processes)

	abstractions := []AbstractionWithName{
		{"app", app.Create(system.messages_queue)},
		{"app.pl", link.Copy().SetParentAbstraction("app")},
	}

	return abstractions
}

func (system *System) register(abstraction *Abstraction, key string) {
	system.abstractions[key] = *abstraction
}

func (system *System) Init() *System {
	initial_abstractions := system.createInitialAbstractions()
	for _, abstraction_with_name := range initial_abstractions {
		system.register(&abstraction_with_name.abstraction, abstraction_with_name.name)
	}

	return system
}

func (system *System) Start() {
	for {
		for message := range system.messages_queue {
			fmt.Printf("System handles message: %s\n\n", message)

			abstraction, exists := system.abstractions[message.ToAbstractionId]

			if !exists {
				// TODO
				err := errors.New("abstraction is not yet supported")
				fmt.Println(err)
			}

			err := abstraction.HandleMessage(message)
			if err != nil {
				// TODO
				err := errors.New("failed to handle message")
				fmt.Println(err)
			}
		}
	}
}

func (system *System) HandleMessage(message *protobuf.Message) {
	system.messages_queue <- message
}

// func (system *System) Add
