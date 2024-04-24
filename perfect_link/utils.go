package perfectlink

import (
	"encoding/binary"

	"github.com/EmanuelPutura/distributed_algo/protobuf"
)

func getWrappedMessage(message *protobuf.Message, sender_host string, sender_port int32, system_id string) protobuf.Message {
	return protobuf.Message{
		SystemId:        system_id,
		ToAbstractionId: message.ToAbstractionId,
		Type:            protobuf.Message_NETWORK_MESSAGE,
		NetworkMessage: &protobuf.NetworkMessage{
			Message: message.PlSend.Message,
			/* Message:             message.NetworkMessage.Message, */
			SenderHost:          sender_host,
			SenderListeningPort: sender_port,
		},
	}
}

func addHeaderToMessageBytes(data []byte) []byte {
	var bytes []byte = make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, uint32(len(data)))
	return append(bytes, data...)
}
