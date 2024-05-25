package nnar

import (
	"errors"
	"fmt"

	dlog "github.com/EmanuelPutura/distributed_algo/log"
	"github.com/EmanuelPutura/distributed_algo/protobuf"
)

type Nnar struct {
	messages_queue chan *protobuf.Message
	n              int32
	key            string
	is_reading     bool
	timestamp      int32
	write_rank     int32
	value          int32
	acks_no        int32
	write_value    *protobuf.Value
	read_id        int32
	read_list      map[int32]*protobuf.NnarInternalValue
}

func Create(
	queue chan *protobuf.Message,
	n int32,
	key string,
	timestamp int32,
	write_rank int32,
	value int32,
	read_list map[int32]*protobuf.NnarInternalValue,
) *Nnar {
	return &Nnar{
		messages_queue: queue,
		n:              n,
		key:            key,
		timestamp:      timestamp,
		write_rank:     write_rank,
		value:          value,
		read_list:      read_list,
	}
}

func (nnar *Nnar) Destroy() {}

func (nnar *Nnar) handleBebDeliverMessage(message *protobuf.Message) *protobuf.Message {
	nnar_base_id := nnar.getAbstractionId()
	switch message.BebDeliver.Message.Type {
	case protobuf.Message_NNAR_INTERNAL_READ:
		return &protobuf.Message{
			Type:              protobuf.Message_PL_SEND,
			FromAbstractionId: nnar_base_id,
			ToAbstractionId:   fmt.Sprintf("%s.pl", nnar_base_id),
			PlSend: &protobuf.PlSend{
				Destination: message.BebDeliver.Sender,
				Message: &protobuf.Message{
					Type:              protobuf.Message_NNAR_INTERNAL_VALUE,
					FromAbstractionId: nnar_base_id,
					ToAbstractionId:   nnar_base_id,
					NnarInternalValue: nnar.buildNnarInternalValue(),
				},
			},
		}
	case protobuf.Message_NNAR_INTERNAL_WRITE:
		beb_deliver_nnar_internal_write := message.BebDeliver.Message.NnarInternalWrite

		incoming_nnar_value := &protobuf.NnarInternalValue{Timestamp: beb_deliver_nnar_internal_write.Timestamp, WriterRank: beb_deliver_nnar_internal_write.WriterRank}
		current_nnar_value := &protobuf.NnarInternalValue{Timestamp: nnar.timestamp, WriterRank: nnar.write_rank}

		if compareNnarInternalValues(incoming_nnar_value, current_nnar_value) == 1 {
			nnar.timestamp = beb_deliver_nnar_internal_write.Timestamp
			nnar.write_rank = beb_deliver_nnar_internal_write.WriterRank
			nnar.updateNnarValue(beb_deliver_nnar_internal_write.Value)
		}

		return &protobuf.Message{
			Type:              protobuf.Message_PL_SEND,
			FromAbstractionId: nnar_base_id,
			ToAbstractionId:   fmt.Sprintf("%s.pl", nnar_base_id),
			PlSend: &protobuf.PlSend{
				Destination: message.BebDeliver.Sender,
				Message: &protobuf.Message{
					Type:              protobuf.Message_NNAR_INTERNAL_ACK,
					FromAbstractionId: nnar_base_id,
					ToAbstractionId:   nnar_base_id,
					NnarInternalAck: &protobuf.NnarInternalAck{
						ReadId: nnar.read_id,
					},
				},
			},
		}
	}

	return nil
}

func (nnar *Nnar) handleNnarWriteMessage(message *protobuf.Message) *protobuf.Message {
	nnar.read_id = nnar.read_id + 1
	nnar.write_value = message.NnarWrite.Value
	nnar.acks_no = 0
	nnar.read_list = make(map[int32]*protobuf.NnarInternalValue)
	nnar_base_id := nnar.getAbstractionId()

	return &protobuf.Message{
		Type:              protobuf.Message_BEB_BROADCAST,
		FromAbstractionId: nnar_base_id,
		ToAbstractionId:   fmt.Sprintf("%s.beb", nnar_base_id),
		BebBroadcast: &protobuf.BebBroadcast{
			Message: &protobuf.Message{
				Type:              protobuf.Message_NNAR_INTERNAL_READ,
				FromAbstractionId: nnar_base_id,
				ToAbstractionId:   nnar_base_id,
				NnarInternalRead: &protobuf.NnarInternalRead{
					ReadId: nnar.read_id,
				},
			},
		},
	}
}

func (nnar *Nnar) handleNnarReadMessage(_ *protobuf.Message) *protobuf.Message {
	nnar.read_id = nnar.read_id + 1
	nnar.acks_no = 0
	nnar.read_list = make(map[int32]*protobuf.NnarInternalValue)
	nnar.is_reading = true
	nnar_base_id := nnar.getAbstractionId()

	return &protobuf.Message{
		Type:              protobuf.Message_BEB_BROADCAST,
		FromAbstractionId: nnar_base_id,
		ToAbstractionId:   fmt.Sprintf("%s.beb", nnar_base_id),
		BebBroadcast: &protobuf.BebBroadcast{
			Message: &protobuf.Message{
				Type:              protobuf.Message_NNAR_INTERNAL_READ,
				FromAbstractionId: nnar_base_id,
				ToAbstractionId:   nnar_base_id,
				NnarInternalRead: &protobuf.NnarInternalRead{
					ReadId: nnar.read_id,
				},
			},
		},
	}
}

func (nnar *Nnar) handleNnarInternalValueMessage(message *protobuf.Message) *protobuf.Message {
	pl_deliver_nnar_internal_write := message.PlDeliver.Message.NnarInternalValue
	nnar_base_id := nnar.getAbstractionId()

	if pl_deliver_nnar_internal_write.ReadId == nnar.read_id {
		nnar.read_list[message.PlDeliver.Sender.Port] = pl_deliver_nnar_internal_write
		nnar.read_list[message.PlDeliver.Sender.Port].WriterRank = message.PlDeliver.Sender.Rank

		if int32(len(nnar.read_list)) > nnar.n/2 {
			h := nnar.highestNnarInternalValue()
			nnar.read_list = make(map[int32]*protobuf.NnarInternalValue)

			if !nnar.is_reading {
				h.Timestamp = h.Timestamp + 1
				h.WriterRank = nnar.write_rank
				h.Value = nnar.write_value
			}

			return &protobuf.Message{
				Type:              protobuf.Message_BEB_BROADCAST,
				FromAbstractionId: nnar_base_id,
				ToAbstractionId:   fmt.Sprintf("%s.beb", nnar_base_id),
				BebBroadcast: &protobuf.BebBroadcast{
					Message: &protobuf.Message{
						Type:              protobuf.Message_NNAR_INTERNAL_WRITE,
						FromAbstractionId: nnar_base_id,
						ToAbstractionId:   nnar_base_id,
						NnarInternalWrite: &protobuf.NnarInternalWrite{
							ReadId:     nnar.read_id,
							Timestamp:  h.Timestamp,
							WriterRank: h.WriterRank,
							Value:      h.Value,
						},
					},
				},
			}
		}
	}

	return nil
}

func (nnar *Nnar) handleNnarInternalAckMessage(message *protobuf.Message) *protobuf.Message {
	nnar_base_id := nnar.getAbstractionId()
	pl_deliver_nnar_internal_write := message.PlDeliver.Message.NnarInternalAck
	if pl_deliver_nnar_internal_write.ReadId == nnar.read_id {
		nnar.acks_no = nnar.acks_no + 1
		if nnar.acks_no > nnar.n/2 {
			nnar.acks_no = 0
			if nnar.is_reading {
				nnar.is_reading = false
				return &protobuf.Message{
					Type:              protobuf.Message_NNAR_READ_RETURN,
					FromAbstractionId: nnar_base_id,
					ToAbstractionId:   "app",
					NnarReadReturn: &protobuf.NnarReadReturn{
						Value: nnar.buildNnarInternalValue().Value,
					},
				}
			} else {
				return &protobuf.Message{
					Type:              protobuf.Message_NNAR_WRITE_RETURN,
					FromAbstractionId: nnar_base_id,
					ToAbstractionId:   "app",
					NnarWriteReturn:   &protobuf.NnarWriteReturn{},
				}
			}
		}
	}

	return nil
}

func (nnar *Nnar) HandleMessage(message *protobuf.Message) error {
	dlog.Dlog.Printf("%-35s Nnar handles message:\n'%s'\n\n", "[NNAR]:", message)
	var queued_message *protobuf.Message = nil

	switch message.Type {
	case protobuf.Message_BEB_DELIVER:
		queued_message = nnar.handleBebDeliverMessage(message)
	case protobuf.Message_NNAR_WRITE:
		queued_message = nnar.handleNnarWriteMessage(message)
	case protobuf.Message_NNAR_READ:
		queued_message = nnar.handleNnarReadMessage(message)
	case protobuf.Message_PL_DELIVER:
		switch message.PlDeliver.Message.Type {
		case protobuf.Message_NNAR_INTERNAL_VALUE:
			queued_message = nnar.handleNnarInternalValueMessage(message)
		case protobuf.Message_NNAR_INTERNAL_ACK:
			queued_message = nnar.handleNnarInternalAckMessage(message)
		}
	default:
		return errors.New("nnar message is not supported")
	}

	if queued_message != nil {
		nnar.messages_queue <- queued_message
	}

	return nil
}

func (nnar *Nnar) getAbstractionId() string {
	return "app.nnar[" + nnar.key + "]"
}

func (nnar *Nnar) getIsValueDefined() bool {
	return nnar.value != -1
}

func (nnar *Nnar) buildNnarInternalValue() *protobuf.NnarInternalValue {
	is_value_defined := nnar.getIsValueDefined()

	return &protobuf.NnarInternalValue{
		ReadId:    nnar.read_id,
		Timestamp: nnar.timestamp,
		Value: &protobuf.Value{
			V:       nnar.value,
			Defined: is_value_defined,
		},
	}
}

func (nnar *Nnar) updateNnarValue(value *protobuf.Value) {
	if value.Defined {
		nnar.value = value.V
	} else {
		nnar.value = -1
	}
}

func (nnar *Nnar) highestNnarInternalValue() *protobuf.NnarInternalValue {
	var res *protobuf.NnarInternalValue = nil
	for _, nnar_value := range nnar.read_list {
		if res == nil {
			res = nnar_value
			continue
		}

		if compareNnarInternalValues(nnar_value, res) == 1 {
			res = nnar_value
		}
	}

	return res
}

func compareNnarInternalValues(value1 *protobuf.NnarInternalValue, value2 *protobuf.NnarInternalValue) int {
	if value1.Timestamp > value2.Timestamp || (value1.Timestamp == value2.Timestamp && value1.WriterRank > value2.WriterRank) {
		return 1
	}

	if value1.Timestamp == value2.Timestamp && value1.WriterRank == value2.WriterRank {
		return 0
	}

	return -1
}
