package perfectlink

import (
	"errors"

	dlog "github.com/EmanuelPutura/distributed_algo/log"
	"github.com/EmanuelPutura/distributed_algo/network"
	"github.com/EmanuelPutura/distributed_algo/protobuf"
	"google.golang.org/protobuf/proto"
)

type PerfectLink struct {
	sender_ip             string
	sender_port           int32
	recv_ip               string
	recv_port             int32
	system_id             string
	parent_abstraction    string
	system_messages_queue chan *protobuf.Message
	system_processes      []*protobuf.ProcessId
}

func Create(sender_ip string, sender_port int32, recv_ip string, recv_port int32) *PerfectLink {
	return &PerfectLink{
		sender_ip:   sender_ip,
		sender_port: sender_port,
		recv_ip:     recv_ip,
		recv_port:   recv_port,
	}
}

func (pl *PerfectLink) Destroy() {

}

func (pl *PerfectLink) SetSystemId(system_id string) *PerfectLink {
	pl.system_id = system_id
	return pl
}

func (pl *PerfectLink) SetSystemMessagesQueue(queue chan *protobuf.Message) *PerfectLink {
	pl.system_messages_queue = queue
	return pl
}

func (pl *PerfectLink) SetSystemProcesses(processes []*protobuf.ProcessId) *PerfectLink {
	pl.system_processes = processes
	return pl
}

func (pl *PerfectLink) SetParentAbstraction(parent_abstraction string) *PerfectLink {
	pl.parent_abstraction = parent_abstraction
	return pl
}

func (pl PerfectLink) Copy() *PerfectLink {
	copy_pl := pl
	return &copy_pl
}

func (pl *PerfectLink) getTcpSendRecvAddress(message *protobuf.Message) (string, int32) {
	if message.PlSend.Destination != nil {
		return message.PlSend.Destination.Host, message.PlSend.Destination.Port
	}

	return pl.recv_ip, pl.recv_port
}

func (pl *PerfectLink) Send(message *protobuf.Message) error {
	var wrapped_message protobuf.Message = getWrappedMessage(message, pl.sender_ip, pl.sender_port, pl.system_id)
	data, err := proto.Marshal(&wrapped_message)
	if err != nil {
		return err
	}

	data = addHeaderToMessageBytes(data)

	recv_ip, recv_port := pl.getTcpSendRecvAddress(message)
	return network.TcpSend(recv_ip, recv_port, data)
}

func (pl *PerfectLink) findSenderProcesse(message *protobuf.Message) *protobuf.ProcessId {
	for _, process := range pl.system_processes {
		if process.Host == message.NetworkMessage.SenderHost && process.Port == message.NetworkMessage.SenderListeningPort {
			return process
		}
	}

	return nil
}

func (pl *PerfectLink) getReadyToDeliverMessage(message *protobuf.Message, sender_process *protobuf.ProcessId) *protobuf.Message {
	return &protobuf.Message{
		SystemId:          message.SystemId,
		FromAbstractionId: message.ToAbstractionId,
		ToAbstractionId:   pl.parent_abstraction,
		Type:              protobuf.Message_PL_DELIVER,
		PlDeliver: &protobuf.PlDeliver{
			Sender:  sender_process,
			Message: message.NetworkMessage.Message,
		},
	}
}

func (pl *PerfectLink) HandleMessage(message *protobuf.Message) error {
	// fmt.Printf("PL handles message:\n%s\n\n", message)
	dlog.Dlog.Printf("PL handles message:\n%s\n\n", message)

	switch message.Type {
	case protobuf.Message_NETWORK_MESSAGE:
		sender_process := pl.findSenderProcesse(message)
		enqueued_message := pl.getReadyToDeliverMessage(message, sender_process)
		pl.system_messages_queue <- enqueued_message
	case protobuf.Message_PL_SEND:
		pl.Send(message)
	default:
		return errors.New("invalid message: message is not supported for perfect links")
	}

	return nil
}
