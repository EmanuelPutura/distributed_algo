package beb

import (
	"errors"
	"fmt"

	dlog "github.com/EmanuelPutura/distributed_algo/log"
	"github.com/EmanuelPutura/distributed_algo/protobuf"
)

type BestEffortBroadcast struct {
	abstraction_id        string
	system_messages_queue chan *protobuf.Message
	system_processes      []*protobuf.ProcessId
}

func Create(abstraction_id string, queue chan *protobuf.Message, processes []*protobuf.ProcessId) *BestEffortBroadcast {
	return &BestEffortBroadcast{
		abstraction_id:        abstraction_id,
		system_messages_queue: queue,
		system_processes:      processes,
	}
}

func (beb *BestEffortBroadcast) Destroy() {

}

func (beb *BestEffortBroadcast) handleMessageBebBroadcast(message *protobuf.Message) {
	for _, process := range beb.system_processes {
		beb.system_messages_queue <- &protobuf.Message{
			Type:              protobuf.Message_PL_SEND,
			FromAbstractionId: beb.abstraction_id,
			ToAbstractionId:   fmt.Sprintf("%s.pl", beb.abstraction_id),
			PlSend: &protobuf.PlSend{
				Destination: process,
				Message:     message.BebBroadcast.Message,
			},
		}
	}
}

func (beb *BestEffortBroadcast) handleMessagePlDeliver(message *protobuf.Message) {
	beb.system_messages_queue <- &protobuf.Message{
		Type:              protobuf.Message_BEB_DELIVER,
		FromAbstractionId: beb.abstraction_id,
		ToAbstractionId:   message.PlDeliver.Message.ToAbstractionId,
		BebDeliver: &protobuf.BebDeliver{
			Sender:  message.PlDeliver.Sender,
			Message: message.PlDeliver.Message,
		},
	}
}

func (beb *BestEffortBroadcast) HandleMessage(message *protobuf.Message) error {
	// fmt.Printf("BEB handles message:\n%s\n\n", message)
	dlog.Dlog.Printf("%-35s BEB handles message:\n%s\n\n", "[BEB]:", message)

	switch message.Type {
	case protobuf.Message_BEB_BROADCAST:
		beb.handleMessageBebBroadcast(message)
	case protobuf.Message_PL_DELIVER:
		beb.handleMessagePlDeliver(message)
	default:
		return errors.New("beb message not supported")
	}

	return nil
}
