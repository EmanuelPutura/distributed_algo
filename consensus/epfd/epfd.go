package epfd

import (
	"errors"
	"fmt"
	"time"

	"github.com/EmanuelPutura/distributed_algo/helpers"
	dlog "github.com/EmanuelPutura/distributed_algo/log"
	"github.com/EmanuelPutura/distributed_algo/protobuf"
)

type EventuallyPerfectFailureDetector struct {
	abstraction_id     string
	parent_abstraction string

	all_processes       []*protobuf.ProcessId
	alive_processes     map[string]*protobuf.ProcessId
	suspected_processes map[string]*protobuf.ProcessId

	delta time.Duration
	delay time.Duration
	timer *time.Timer

	messages_queue chan *protobuf.Message
}

func Create(
	abstraction_id string,
	parent_abstraction string,
	messages_queue chan *protobuf.Message,
	all_processes []*protobuf.ProcessId,
	delta time.Duration,
) *EventuallyPerfectFailureDetector {
	var epfd *EventuallyPerfectFailureDetector = &EventuallyPerfectFailureDetector{
		abstraction_id:      abstraction_id,
		parent_abstraction:  parent_abstraction,
		all_processes:       all_processes,
		alive_processes:     make(map[string]*protobuf.ProcessId),
		suspected_processes: make(map[string]*protobuf.ProcessId),
		delta:               delta,
		delay:               delta,
		timer:               time.NewTimer(delta),
		messages_queue:      messages_queue,
	}

	for _, process := range all_processes {
		epfd.alive_processes[helpers.GetProcessName(process)] = process
	}

	StartTimer(epfd, epfd.delay)
	return epfd
}

func (epfd *EventuallyPerfectFailureDetector) Destroy() {
	epfd.timer.Stop()
}

func (epfd *EventuallyPerfectFailureDetector) handleEpfdTimeout() {
	for process_name := range epfd.suspected_processes {
		if _, exists := epfd.alive_processes[process_name]; exists {
			epfd.delay = epfd.delay + epfd.delta
			dlog.Dlog.Printf("%-35s EPFD increases timeout value to [%v]\n\n", "[EPFD]:", epfd.delay)
			break
		}
	}

	for _, process_id := range epfd.all_processes {
		process_name := helpers.GetProcessName(process_id)
		_, is_alive := epfd.alive_processes[process_name]
		_, is_suspected := epfd.suspected_processes[process_name]

		if !is_alive && !is_suspected {
			epfd.suspected_processes[process_name] = process_id

			// Send EPFD process suspicion
			epfd.messages_queue <- &protobuf.Message{
				Type:              protobuf.Message_EPFD_SUSPECT,
				FromAbstractionId: epfd.abstraction_id,
				ToAbstractionId:   epfd.parent_abstraction,
				EpfdSuspect: &protobuf.EpfdSuspect{
					Process: process_id,
				},
			}
		} else if is_alive && is_suspected {
			delete(epfd.suspected_processes, process_name)

			// Send EPFD restore
			epfd.messages_queue <- &protobuf.Message{
				Type:              protobuf.Message_EPFD_RESTORE,
				FromAbstractionId: epfd.abstraction_id,
				ToAbstractionId:   epfd.parent_abstraction,
				EpfdRestore: &protobuf.EpfdRestore{
					Process: process_id,
				},
			}
		}

		// Request heartbeat
		epfd.messages_queue <- &protobuf.Message{
			Type:              protobuf.Message_PL_SEND,
			FromAbstractionId: epfd.abstraction_id,
			ToAbstractionId:   fmt.Sprintf("%s.pl", epfd.abstraction_id),
			PlSend: &protobuf.PlSend{
				Destination: process_id,
				Message: &protobuf.Message{
					Type:                         protobuf.Message_EPFD_INTERNAL_HEARTBEAT_REQUEST,
					FromAbstractionId:            epfd.abstraction_id,
					ToAbstractionId:              epfd.abstraction_id,
					EpfdInternalHeartbeatRequest: &protobuf.EpfdInternalHeartbeatRequest{},
				},
			},
		}
	}

	epfd.alive_processes = make(map[string]*protobuf.ProcessId)
	StartTimer(epfd, epfd.delay)
}

func (epfd *EventuallyPerfectFailureDetector) handlePerfectLinkLayerPlDeliver() *protobuf.Message {
	return &protobuf.Message{
		Type:              protobuf.Message_PL_SEND,
		FromAbstractionId: epfd.abstraction_id,
		ToAbstractionId:   fmt.Sprintf("%s.pl", epfd.abstraction_id),
		PlSend: &protobuf.PlSend{
			Message: &protobuf.Message{
				Type:                       protobuf.Message_EPFD_INTERNAL_HEARTBEAT_REPLY,
				FromAbstractionId:          epfd.abstraction_id,
				ToAbstractionId:            epfd.abstraction_id,
				EpfdInternalHeartbeatReply: &protobuf.EpfdInternalHeartbeatReply{},
			},
		},
	}
}

func (epfd *EventuallyPerfectFailureDetector) HandleMessage(message *protobuf.Message) error {
	if !(message.Type == protobuf.Message_PL_DELIVER &&
		(message.PlDeliver.Message.Type == protobuf.Message_EPFD_INTERNAL_HEARTBEAT_REQUEST || message.PlDeliver.Message.Type == protobuf.Message_EPFD_INTERNAL_HEARTBEAT_REPLY)) &&
		message.Type != protobuf.Message_EPFD_TIMEOUT {
		dlog.Dlog.Printf("%-35s EPFD handles message:\n%s\n\n", "[EPFD]:", message)
	}

	switch message.Type {
	case protobuf.Message_EPFD_TIMEOUT:
		epfd.handleEpfdTimeout()
	case protobuf.Message_PL_DELIVER:
		switch message.PlDeliver.Message.Type {
		case protobuf.Message_EPFD_INTERNAL_HEARTBEAT_REQUEST:
			epfd.messages_queue <- epfd.handlePerfectLinkLayerPlDeliver()
		case protobuf.Message_EPFD_INTERNAL_HEARTBEAT_REPLY:
			epfd.alive_processes[helpers.GetProcessName(message.PlDeliver.Sender)] = message.PlDeliver.Sender
		default:
			return errors.New("message not supported")
		}
	default:
		return errors.New("invalid message: message is not supported for EPFD")
	}

	return nil
}
