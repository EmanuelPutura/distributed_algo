package admin

import (
	"encoding/binary"

	"github.com/EmanuelPutura/distributed_algo/network"
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

func getWrappedMessage(message *protobuf.Message, sender_host string, sender_listening_port int32) protobuf.Message {
	return protobuf.Message{
		SystemId:        "",
		ToAbstractionId: message.ToAbstractionId,
		Type:            protobuf.Message_NETWORK_MESSAGE,
		NetworkMessage: &protobuf.NetworkMessage{
			Message: message.PlSend.Message,
			/* Message:             message.NetworkMessage.Message, */
			SenderHost:          sender_host,
			SenderListeningPort: sender_listening_port,
		},
	}
}

func addHeaderToMessageBytes(data []byte) []byte {
	var bytes []byte = make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, uint32(len(data)))
	return append(bytes, data...)
}

func RegisterProcess(hub_ip string, hub_port int32, proc_ip string, proc_port int32, owner string, index int32) error {
	var proc_registration_message protobuf.Message = getProcRegistrationMessage(owner, index)

	var send_msg protobuf.Message = protobuf.Message{
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

	var register_msg protobuf.Message = getWrappedMessage(&send_msg, proc_ip, proc_port)

	data, err := proto.Marshal(&register_msg)
	if err != nil {
		return err
	}

	data = addHeaderToMessageBytes(data)
	err = network.TcpSend(hub_ip, hub_port, data)

	return err
}

func ParseNetworkMessage(data []byte) (*protobuf.Message, error) {
	var message *protobuf.Message = &protobuf.Message{}
	var err error = proto.Unmarshal(data, message)
	if err != nil {
		return nil, err
	}

	return message, nil
}
