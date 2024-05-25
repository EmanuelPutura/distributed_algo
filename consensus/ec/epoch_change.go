package ec

import (
	"errors"
	"fmt"

	"github.com/EmanuelPutura/distributed_algo/helpers"
	dlog "github.com/EmanuelPutura/distributed_algo/log"
	"github.com/EmanuelPutura/distributed_algo/protobuf"
)

type EpochChange struct {
	abstraction_id     string
	parent_abstraction string

	all_processes   []*protobuf.ProcessId
	trusted_process *protobuf.ProcessId
	process_id      *protobuf.ProcessId

	last_timestamp    int32
	current_timestamp int32

	messages_queue chan *protobuf.Message
}

func Create(
	abstraction_id string,
	parent_abstraction string,
	process_id *protobuf.ProcessId,
	messages_queue chan *protobuf.Message,
	all_processes []*protobuf.ProcessId,
) *EpochChange {
	return &EpochChange{
		abstraction_id:     abstraction_id,
		parent_abstraction: parent_abstraction,
		all_processes:      all_processes,
		trusted_process:    helpers.GetProcessMaxRankFromSlice(all_processes),
		process_id:         process_id,
		messages_queue:     messages_queue,
		last_timestamp:     0,
		current_timestamp:  process_id.Rank,
	}
}

func (ec *EpochChange) Destroy() {
}

func (ec *EpochChange) handleEldLayerTrust(message *protobuf.Message) *protobuf.Message {
	ec.trusted_process = message.EldTrust.Process

	if helpers.GetProcessName(ec.process_id) == helpers.GetProcessName(ec.trusted_process) {
		ec.current_timestamp = ec.current_timestamp + int32(len(ec.all_processes))
		return &protobuf.Message{
			Type:              protobuf.Message_BEB_BROADCAST,
			FromAbstractionId: ec.abstraction_id,
			ToAbstractionId:   fmt.Sprintf("%s.beb", ec.abstraction_id),
			BebBroadcast: &protobuf.BebBroadcast{
				Message: &protobuf.Message{
					Type:              protobuf.Message_EC_INTERNAL_NEW_EPOCH,
					FromAbstractionId: ec.abstraction_id,
					ToAbstractionId:   ec.abstraction_id,
					EcInternalNewEpoch: &protobuf.EcInternalNewEpoch{
						Timestamp: ec.current_timestamp,
					},
				},
			},
		}
	}

	return nil
}

func (ec *EpochChange) handlePerfectLinkLayerDeliver(message *protobuf.Message) *protobuf.Message {
	switch message.PlDeliver.Message.Type {
	case protobuf.Message_EC_INTERNAL_NACK:
		return ec.handleEldLayerTrust(message)
	}

	return nil
}

func (ec *EpochChange) handleBebLayerDeliver(message *protobuf.Message) *protobuf.Message {
	switch message.BebDeliver.Message.Type {
	case protobuf.Message_EC_INTERNAL_NEW_EPOCH:
		updated_timestamp := message.BebDeliver.Message.EcInternalNewEpoch.Timestamp
		sender_process_name := helpers.GetProcessName(message.BebDeliver.Sender)
		trusted_process_name := helpers.GetProcessName(ec.trusted_process)

		if sender_process_name == trusted_process_name && updated_timestamp > ec.last_timestamp {
			ec.last_timestamp = updated_timestamp
			return &protobuf.Message{
				Type:              protobuf.Message_EC_START_EPOCH,
				FromAbstractionId: ec.abstraction_id,
				ToAbstractionId:   ec.parent_abstraction,
				EcStartEpoch: &protobuf.EcStartEpoch{
					NewTimestamp: updated_timestamp,
					NewLeader:    message.BebDeliver.Sender,
				},
			}
		} else {
			return &protobuf.Message{
				Type:              protobuf.Message_PL_SEND,
				FromAbstractionId: ec.abstraction_id,
				ToAbstractionId:   fmt.Sprintf("%s.pl", ec.abstraction_id),
				PlSend: &protobuf.PlSend{
					Destination: message.BebDeliver.Sender,
					Message: &protobuf.Message{
						Type:              protobuf.Message_EC_INTERNAL_NACK,
						FromAbstractionId: ec.abstraction_id,
						ToAbstractionId:   ec.abstraction_id,
						EcInternalNack:    &protobuf.EcInternalNack{},
					},
				},
			}
		}
	}

	return nil
}

func (ec *EpochChange) HandleMessage(message *protobuf.Message) error {
	dlog.Dlog.Printf("%-35s EC handles message:\n%s\n\n", "[Epoch Change]:", message)
	var queued_message *protobuf.Message = nil

	switch message.Type {
	case protobuf.Message_ELD_TRUST:
		queued_message = ec.handleEldLayerTrust(message)
	case protobuf.Message_PL_DELIVER:
		queued_message = ec.handlePerfectLinkLayerDeliver(message)
	case protobuf.Message_BEB_DELIVER:
		queued_message = ec.handleBebLayerDeliver(message)
	default:
		return errors.New("invalid message: message is not supported for EC")
	}

	if queued_message != nil {
		ec.messages_queue <- queued_message
	}

	return nil
}
