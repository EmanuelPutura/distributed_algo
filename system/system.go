package system

import (
	"fmt"
	"os"
	"strings"

	"github.com/EmanuelPutura/distributed_algo/abstraction"
	"github.com/EmanuelPutura/distributed_algo/app"
	"github.com/EmanuelPutura/distributed_algo/beb"
	"github.com/EmanuelPutura/distributed_algo/helpers"
	dlog "github.com/EmanuelPutura/distributed_algo/log"
	"github.com/EmanuelPutura/distributed_algo/nnar"
	perfectlink "github.com/EmanuelPutura/distributed_algo/perfect_link"
	"github.com/EmanuelPutura/distributed_algo/protobuf"
)

type System struct {
	id             string
	hub_ip         string
	hub_port       int32
	messages_queue chan *protobuf.Message
	parent_process *protobuf.ProcessId
	all_processes  []*protobuf.ProcessId
	abstractions   map[string]abstraction.Abstraction
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
		id:             message.SystemId,
		hub_ip:         hub_ip,
		hub_port:       hub_port,
		messages_queue: make(chan *protobuf.Message, 4096),
		parent_process: parent_process,
		all_processes:  message.ProcInitializeSystem.Processes,
		abstractions:   make(map[string]abstraction.Abstraction),
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
	abstraction abstraction.Abstraction
}

func (system *System) createPerfectLink() *perfectlink.PerfectLink {
	return perfectlink.Create(
		system.parent_process.Host,
		system.parent_process.Port,
		system.hub_ip,
		system.hub_port,
	).SetSystemId(system.id).SetSystemMessagesQueue(system.messages_queue).SetSystemProcesses(system.all_processes)
}

func (system *System) createInitialAbstractions() []AbstractionWithName {
	var link *perfectlink.PerfectLink = system.createPerfectLink()
	abstractions := []AbstractionWithName{
		{"app", app.Create(system.messages_queue)},
		{"app.pl", link.Copy().SetParentAbstraction("app")},
		{"app.beb", beb.Create("app.beb", system.messages_queue, system.all_processes)},
		{"app.beb.pl", link.Copy().SetParentAbstraction("app.beb")},
	}

	return abstractions
}

func (system *System) createNnarAbstractions(key string) {
	var link *perfectlink.PerfectLink = system.createPerfectLink()
	nnar_base_id := fmt.Sprintf("app.nnar[%s]", key)

	system.abstractions[nnar_base_id] = nnar.Create(
		system.messages_queue,
		int32(len(system.all_processes)),
		key,
		0,
		system.parent_process.Rank,
		-1,
		make(map[int32]*protobuf.NnarInternalValue),
	)

	beb_id := fmt.Sprintf("%s.beb", nnar_base_id)
	system.abstractions[fmt.Sprintf("%s.pl", nnar_base_id)] = link.Copy().SetParentAbstraction(nnar_base_id)
	system.abstractions[beb_id] = beb.Create(beb_id, system.messages_queue, system.all_processes)
	system.abstractions[fmt.Sprintf("%s.beb.pl", nnar_base_id)] = link.Copy().SetParentAbstraction(beb_id)
}

func (system *System) createConsensusAbstractions(key string) {
	// var link *perfectlink.PerfectLink = system.createPerfectLink()
	// consensus_base_id := fmt.Sprintf("app.uc[%s]", key)

	// TODO: add abstractions
}

func (system *System) register(abstraction *abstraction.Abstraction, key string) {
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
			// fmt.Printf("System handles message:\n%s\n\n", message)
			dlog.Dlog.Printf("%-35s System handles message: %s\n\n", "[system]:", message)

			_, exists := system.abstractions[message.ToAbstractionId]

			if !exists {
				fmt.Printf("\n\n---------> %s\n\n", message.ToAbstractionId)

				// Create nnar abstractions here because each abstraction is specific to the corresponding nnar register
				if strings.HasPrefix(message.ToAbstractionId, "app.nnar") {
					system.createNnarAbstractions(helpers.RetrieveIdFromAbstraction((message.ToAbstractionId)))
				}
				if message.Type == protobuf.Message_UC_PROPOSE {
					system.createConsensusAbstractions(helpers.RetrieveIdFromAbstraction((message.ToAbstractionId)))
				}
			}

			abstraction, exists := system.abstractions[message.ToAbstractionId]
			if !exists {
				err := fmt.Errorf("%-35s Failed to handle message", "[error]:")
				dlog.Dlog.Println(err)
				os.Exit(-1)
			}

			err := abstraction.HandleMessage(message)
			if err != nil {
				err := fmt.Errorf("%-35s Failed to handle message", "[error]:")
				dlog.Dlog.Println(err)
				os.Exit(-1)
			}
		}
	}
}

func (system *System) HandleMessage(message *protobuf.Message) {
	system.messages_queue <- message
}
