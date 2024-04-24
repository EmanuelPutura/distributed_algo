package perfectlink

import (
	"errors"

	"github.com/EmanuelPutura/distributed_algo/network"
	"github.com/EmanuelPutura/distributed_algo/protobuf"
	"google.golang.org/protobuf/proto"
)

type PerfectLink struct {
	sender_ip   string
	sender_port int32
	recv_ip     string
	recv_port   int32
	system_id   string
}

func Create(sender_ip string, sender_port int32, recv_ip string, recv_port int32) *PerfectLink {
	return &PerfectLink{
		sender_ip:   sender_ip,
		sender_port: sender_port,
		recv_ip:     recv_ip,
		recv_port:   recv_port,
	}
}

func (pl *PerfectLink) Send(message *protobuf.Message) error {
	var wrapped_message protobuf.Message = getWrappedMessage(message, pl.sender_ip, pl.sender_port, pl.system_id)

	data, err := proto.Marshal(&wrapped_message)
	if err != nil {
		return err
	}

	data = addHeaderToMessageBytes(data)
	return network.TcpSend(pl.recv_ip, pl.recv_port, data)
}

func (pl *PerfectLink) Handle(message *protobuf.Message) error {
	switch message.Type {
	case protobuf.Message_NETWORK_MESSAGE:

	default:
		return errors.New("invalid message: message is not supported for perfect links")
	}

	return nil
}
