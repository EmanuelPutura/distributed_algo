package eld

import (
	"errors"

	"github.com/EmanuelPutura/distributed_algo/helpers"
	dlog "github.com/EmanuelPutura/distributed_algo/log"
	"github.com/EmanuelPutura/distributed_algo/protobuf"
)

type EventualLeaderDetector struct {
	abstraction_id     string
	parent_abstraction string

	all_processes   []*protobuf.ProcessId
	alive_processes map[string]*protobuf.ProcessId
	leader          *protobuf.ProcessId

	messages_queue chan *protobuf.Message
}

func Create(
	abstraction_id string,
	parent_abstraction string,
	messages_queue chan *protobuf.Message,
	all_processes []*protobuf.ProcessId,
) *EventualLeaderDetector {
	var eld *EventualLeaderDetector = &EventualLeaderDetector{
		abstraction_id:     abstraction_id,
		parent_abstraction: parent_abstraction,
		all_processes:      all_processes,
		alive_processes:    make(map[string]*protobuf.ProcessId),
		leader:             nil,
		messages_queue:     messages_queue,
	}

	for _, process := range all_processes {
		eld.alive_processes[helpers.GetProcessName(process)] = process
	}

	return eld
}

func (epfd *EventualLeaderDetector) Destroy() {
}

func (eld *EventualLeaderDetector) updateLeader() error {
	max_rank_process := helpers.GetProcessMaxRank(eld.alive_processes)
	if max_rank_process == nil {
		return errors.New("cannot find any max rank process")
	}

	if eld.leader == nil || helpers.GetProcessName(eld.leader) != helpers.GetProcessName(max_rank_process) {
		eld.leader = max_rank_process
		eld.messages_queue <- &protobuf.Message{
			Type:              protobuf.Message_ELD_TRUST,
			FromAbstractionId: eld.abstraction_id,
			ToAbstractionId:   eld.parent_abstraction,
			EldTrust: &protobuf.EldTrust{
				Process: eld.leader,
			},
		}
	}

	return nil
}

func (eld *EventualLeaderDetector) HandleMessage(message *protobuf.Message) error {
	dlog.Dlog.Printf("%-35s ELD handles message:\n%s\n\n", "[ELD]:", message)

	// TODO: need to check this
	switch message.Type {
	case protobuf.Message_EPFD_SUSPECT:
		key := helpers.GetProcessName(message.EpfdSuspect.Process)
		delete(eld.alive_processes, key)
	case protobuf.Message_EPFD_RESTORE:
		eld.alive_processes[helpers.GetProcessName(message.EpfdRestore.Process)] = message.EpfdRestore.Process
	default:
		return errors.New("invalid message: message is not supported for EPFD")
	}

	return eld.updateLeader()
}
