package uc

import (
	"errors"

	"github.com/EmanuelPutura/distributed_algo/abstraction"
	"github.com/EmanuelPutura/distributed_algo/consensus/ep"
	"github.com/EmanuelPutura/distributed_algo/helpers"
	dlog "github.com/EmanuelPutura/distributed_algo/log"
	perfectlink "github.com/EmanuelPutura/distributed_algo/perfect_link"
	"github.com/EmanuelPutura/distributed_algo/protobuf"
)

type UniformConsensus struct {
	abstraction_id string
	process_id     *protobuf.ProcessId

	all_abstractions map[string]abstraction.Abstraction
	value            *protobuf.Value
	perfect_link     *perfectlink.PerfectLink

	all_processes []*protobuf.ProcessId
	leader        *protobuf.ProcessId
	new_leader    *protobuf.ProcessId

	epoch_timestamp int32
	new_timestamp   int32
	proposed        bool
	decided         bool

	messages_queue chan *protobuf.Message
}

func Create(
	abstraction_id string,
	process_id *protobuf.ProcessId,
	all_abstractions map[string]abstraction.Abstraction,
	perfect_link *perfectlink.PerfectLink,
	all_processes []*protobuf.ProcessId,
	messages_queue chan *protobuf.Message,
) *UniformConsensus {
	return UniformConsensus{
		abstraction_id:   abstraction_id,
		process_id:       process_id,
		all_abstractions: all_abstractions,
		value:            &protobuf.Value{},
		perfect_link:     perfect_link,
		all_processes:    all_processes,
		leader:           helpers.GetProcessMaxRankFromSlice(all_processes),
		new_leader:       &protobuf.ProcessId{},
		epoch_timestamp:  0,
		new_timestamp:    0,
		proposed:         false,
		decided:          false,
		messages_queue:   messages_queue,
	}.initAbstractions(&ep.EpValueState{})
}

func (uc *UniformConsensus) Destroy() {
}

func (uc *UniformConsensus) updateLeader() *protobuf.Message {
	if helpers.GetProcessName(uc.leader) == helpers.GetProcessName(uc.process_id) && uc.value.Defined && !uc.proposed {
		uc.proposed = true
		return &protobuf.Message{
			Type:              protobuf.Message_EP_PROPOSE,
			FromAbstractionId: uc.abstraction_id,
			ToAbstractionId:   uc.getId(),
			EpPropose: &protobuf.EpPropose{
				Value: uc.value,
			},
		}
	}

	return nil
}

func (uc *UniformConsensus) updateMessagesQueue(queued_message *protobuf.Message) {
	if queued_message != nil {
		uc.messages_queue <- queued_message
	}
}

func (uc *UniformConsensus) handleEcStartEpoch(message *protobuf.Message) *protobuf.Message {
	uc.new_timestamp = message.EcStartEpoch.NewTimestamp
	uc.new_leader = message.EcStartEpoch.NewLeader

	return &protobuf.Message{
		Type:              protobuf.Message_EP_ABORT,
		FromAbstractionId: uc.abstraction_id,
		ToAbstractionId:   uc.getId(),
		EpAbort:           &protobuf.EpAbort{},
	}
}

func (uc *UniformConsensus) handleEcEpAborted(message *protobuf.Message) {
	if uc.epoch_timestamp == message.EpAborted.Ets {
		uc.epoch_timestamp = uc.new_timestamp
		uc.leader = uc.new_leader
		uc.proposed = false

		uc.initAbstractions(ep.CreateEpValueState(uc.epoch_timestamp, message.EpAborted.Value))
	}
}

func (uc *UniformConsensus) handleEpDecide(message *protobuf.Message) *protobuf.Message {
	if uc.epoch_timestamp == message.EpDecide.Ets && !uc.decided {
		uc.decided = true
		return &protobuf.Message{
			Type:              protobuf.Message_UC_DECIDE,
			FromAbstractionId: uc.abstraction_id,
			ToAbstractionId:   "app",
			UcDecide: &protobuf.UcDecide{
				Value: message.EpDecide.Value,
			},
		}
	}

	return nil
}

func (uc *UniformConsensus) HandleMessage(message *protobuf.Message) error {
	dlog.Dlog.Printf("%-35s UC handles message:\n%s\n\n", "[Uniform Consensus]:", message)
	var queued_message *protobuf.Message = nil

	switch message.Type {
	case protobuf.Message_UC_PROPOSE:
		uc.value = message.UcPropose.Value
	case protobuf.Message_EC_START_EPOCH:
		queued_message = uc.handleEcStartEpoch(message)
	case protobuf.Message_EP_ABORTED:
		uc.handleEcEpAborted(message)
	case protobuf.Message_EP_DECIDE:
		queued_message = uc.handleEpDecide(message)
	default:
		return errors.New("invalid message: message is not supported for EP")
	}

	uc.updateMessagesQueue(queued_message)
	uc.updateMessagesQueue(uc.updateLeader())

	return nil
}
