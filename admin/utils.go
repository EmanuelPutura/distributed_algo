package admin

import (
	perfectlink "github.com/EmanuelPutura/distributed_algo/perfect_link"
	"github.com/EmanuelPutura/distributed_algo/protobuf"
	"google.golang.org/protobuf/proto"
)

func getProcRegistrationMessage(owner string, index int32) protobuf.Message {
	return protobuf.Message{
		Type: protobuf.Message_PROC_REGISTRATION,
		ProcRegistration: &protobuf.ProcRegistration{
			Owner: owner,
			Index: index,
		},
	}
}

func RegisterProcess(hub_ip string, hub_port int32, proc_ip string, proc_port int32, owner string, index int32) error {
	var proc_registration_message protobuf.Message = getProcRegistrationMessage(owner, index)

	var pl_send_message protobuf.Message = protobuf.Message{
		Type: protobuf.Message_PL_SEND,
		PlSend: &protobuf.PlSend{
			Destination: &protobuf.ProcessId{
				Host: hub_ip,
				Port: hub_port,
			},
			Message: &proc_registration_message,
		},
	}

	/*
		var send_msg protobuf.Message = protobuf.Message{
			Type: protobuf.Message_PL_SEND,
			NetworkMessage: &protobuf.NetworkMessage{
				SenderHost:          proc_ip,
				SenderListeningPort: proc_port,
				Message:             &proc_registration_message,
			},
		}
	*/

	var link *perfectlink.PerfectLink = perfectlink.Create(proc_ip, proc_port, hub_ip, hub_port)
	return link.Send(&pl_send_message)
}

func ParseNetworkMessage(data []byte) (*protobuf.Message, error) {
	var message *protobuf.Message = &protobuf.Message{}
	var err error = proto.Unmarshal(data, message)
	if err != nil {
		return nil, err
	}

	return message, nil
}
