package ep

import (
	"errors"
	"fmt"

	"github.com/EmanuelPutura/distributed_algo/helpers"
	dlog "github.com/EmanuelPutura/distributed_algo/log"
	"github.com/EmanuelPutura/distributed_algo/protobuf"
)

type EpochConsensus struct {
	abstraction_id     string
	parent_abstraction string

	epoch_timestamp int32
	is_aborted      bool
	accepted        int

	value_state      *EpValueState
	tmp_value        *protobuf.Value
	all_value_states map[string]*EpValueState

	all_processes  []*protobuf.ProcessId
	messages_queue chan *protobuf.Message
}

func Create(
	abstraction_id string,
	parent_abstraction string,
	epoch_timestamp int32,
	value_state *EpValueState,
	messages_queue chan *protobuf.Message,
	all_processes []*protobuf.ProcessId,
) *EpochConsensus {
	return &EpochConsensus{
		abstraction_id:     abstraction_id,
		parent_abstraction: parent_abstraction,
		epoch_timestamp:    epoch_timestamp,
		is_aborted:         false,
		accepted:           0,
		value_state:        value_state,
		tmp_value:          &protobuf.Value{},
		all_value_states:   make(map[string]*EpValueState),
		all_processes:      all_processes,
		messages_queue:     messages_queue,
	}
}

func (ep *EpochConsensus) Destroy() {
}

func (ep *EpochConsensus) handleEpPropose(message *protobuf.Message) *protobuf.Message {
	ep.tmp_value = message.EpPropose.Value
	return &protobuf.Message{
		Type:              protobuf.Message_BEB_BROADCAST,
		FromAbstractionId: ep.abstraction_id,
		ToAbstractionId:   fmt.Sprintf("%s.beb", ep.abstraction_id),
		BebBroadcast: &protobuf.BebBroadcast{
			Message: &protobuf.Message{
				Type:              protobuf.Message_EP_INTERNAL_READ,
				FromAbstractionId: ep.abstraction_id,
				ToAbstractionId:   ep.abstraction_id,
				EpInternalRead:    &protobuf.EpInternalRead{},
			},
		},
	}
}

func (ep *EpochConsensus) handlePlDeliver(message *protobuf.Message) *protobuf.Message {
	switch message.PlDeliver.Message.Type {
	case protobuf.Message_EP_INTERNAL_STATE:
		ep.all_value_states[helpers.GetProcessName(message.PlDeliver.Sender)] = &EpValueState{
			value_timestamp: message.PlDeliver.Message.EpInternalState.ValueTimestamp,
			value:           message.PlDeliver.Message.EpInternalState.Value,
		}

		if len(ep.all_value_states) > len(ep.all_processes)/2 {
			highest := ep.highestValueState()
			if highest != nil && highest.value != nil && highest.value.Defined {
				ep.tmp_value = highest.value
			}

			ep.all_value_states = make(map[string]*EpValueState)
			return &protobuf.Message{
				Type:              protobuf.Message_BEB_BROADCAST,
				FromAbstractionId: ep.abstraction_id,
				ToAbstractionId:   fmt.Sprintf("%s.beb", ep.abstraction_id),
				BebBroadcast: &protobuf.BebBroadcast{
					Message: &protobuf.Message{
						Type:              protobuf.Message_EP_INTERNAL_WRITE,
						FromAbstractionId: ep.abstraction_id,
						ToAbstractionId:   ep.abstraction_id,
						EpInternalWrite: &protobuf.EpInternalWrite{
							Value: ep.tmp_value,
						},
					},
				},
			}
		}
	case protobuf.Message_EP_INTERNAL_ACCEPT:
		ep.accepted = ep.accepted + 1
		if ep.accepted > len(ep.all_processes)/2 {
			ep.accepted = 0
			return &protobuf.Message{
				Type:              protobuf.Message_BEB_BROADCAST,
				FromAbstractionId: ep.abstraction_id,
				ToAbstractionId:   fmt.Sprintf("%s.beb", ep.abstraction_id),
				BebBroadcast: &protobuf.BebBroadcast{
					Message: &protobuf.Message{
						Type:              protobuf.Message_EP_INTERNAL_DECIDED,
						FromAbstractionId: ep.abstraction_id,
						ToAbstractionId:   ep.abstraction_id,
						EpInternalDecided: &protobuf.EpInternalDecided{
							Value: ep.tmp_value,
						},
					},
				},
			}
		}
	}

	return nil
}

func (ep *EpochConsensus) handleBebDeliver(message *protobuf.Message) *protobuf.Message {
	switch message.BebDeliver.Message.Type {
	case protobuf.Message_EP_INTERNAL_READ:
		return &protobuf.Message{
			Type:              protobuf.Message_PL_SEND,
			FromAbstractionId: ep.abstraction_id,
			ToAbstractionId:   fmt.Sprintf("%s.pl", ep.abstraction_id),
			PlSend: &protobuf.PlSend{
				Destination: message.BebDeliver.Sender,
				Message: &protobuf.Message{
					Type:              protobuf.Message_EP_INTERNAL_STATE,
					FromAbstractionId: ep.abstraction_id,
					ToAbstractionId:   ep.abstraction_id,
					EpInternalState: &protobuf.EpInternalState{
						ValueTimestamp: ep.value_state.value_timestamp,
						Value:          ep.value_state.value,
					},
				},
			},
		}
	case protobuf.Message_EP_INTERNAL_WRITE:
		ep.value_state.value_timestamp = ep.epoch_timestamp
		ep.value_state.value = message.BebDeliver.Message.EpInternalWrite.Value
		return &protobuf.Message{
			Type:              protobuf.Message_PL_SEND,
			FromAbstractionId: ep.abstraction_id,
			ToAbstractionId:   fmt.Sprintf("%s.pl", ep.abstraction_id),
			PlSend: &protobuf.PlSend{
				Destination: message.BebDeliver.Sender,
				Message: &protobuf.Message{
					Type:              protobuf.Message_EP_INTERNAL_ACCEPT,
					FromAbstractionId: ep.abstraction_id,
					ToAbstractionId:   ep.abstraction_id,
					EpInternalAccept:  &protobuf.EpInternalAccept{},
				},
			},
		}
	case protobuf.Message_EP_INTERNAL_DECIDED:
		return &protobuf.Message{
			Type:              protobuf.Message_EP_DECIDE,
			FromAbstractionId: ep.abstraction_id,
			ToAbstractionId:   ep.parent_abstraction,
			EpDecide: &protobuf.EpDecide{
				Ets:   ep.epoch_timestamp,
				Value: ep.value_state.value,
			},
		}
	}

	return nil
}

func (ep *EpochConsensus) handleEpAbort() *protobuf.Message {
	ep.is_aborted = true

	return &protobuf.Message{
		Type:              protobuf.Message_EP_ABORTED,
		FromAbstractionId: ep.abstraction_id,
		ToAbstractionId:   ep.parent_abstraction,
		EpAborted: &protobuf.EpAborted{
			Ets:            ep.epoch_timestamp,
			ValueTimestamp: ep.value_state.value_timestamp,
			Value:          ep.value_state.value,
		},
	}
}

func (ep *EpochConsensus) HandleMessage(message *protobuf.Message) error {
	dlog.Dlog.Printf("%-35s EP handles message:\n%s\n\n", "[Epoch Consensus]:", message)
	var queued_message *protobuf.Message = nil

	switch message.Type {
	case protobuf.Message_EP_PROPOSE:
		queued_message = ep.handleEpPropose(message)
	case protobuf.Message_PL_DELIVER:
		queued_message = ep.handlePlDeliver(message)
	case protobuf.Message_BEB_DELIVER:
		queued_message = ep.handleBebDeliver(message)
	case protobuf.Message_EP_ABORT:
		queued_message = ep.handleEpAbort()
	default:
		return errors.New("invalid message: message is not supported for EP")
	}

	if queued_message != nil {
		ep.messages_queue <- queued_message
	}

	return nil
}
