package app

import (
	"fmt"

	"github.com/EmanuelPutura/distributed_algo/protobuf"
)

type App struct {
	messages_queue chan *protobuf.Message
}

func Create(queue chan *protobuf.Message) *App {
	return &App{
		messages_queue: queue,
	}
}

func (app *App) handlePerfectLinkLayerDeliver(message *protobuf.Message) *protobuf.Message {
	return message
}

func (app *App) HandleMessage(message *protobuf.Message) error {
	// var queued_message *protobuf.Message
	fmt.Printf("App receieved message:\n%s\n\n", message)

	switch message.Type {
	case protobuf.Message_PL_DELIVER:

	}

	return nil
}

func (app *App) Destroy() {

}
