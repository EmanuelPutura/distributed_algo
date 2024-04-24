package abstraction

import "github.com/EmanuelPutura/distributed_algo/protobuf"

type Abstraction interface {
	HandleMessage(message *protobuf.Message) error
	Destroy()
}
