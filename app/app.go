package app

import (
	"errors"
	"fmt"

	"github.com/EmanuelPutura/distributed_algo/protobuf"
)

type App struct {
	abstraction_id string
	messages_queue chan *protobuf.Message
}

func Create(queue chan *protobuf.Message) *App {
	return &App{
		abstraction_id: "app",
		messages_queue: queue,
	}
}

func (app *App) handlePerfectLinkLayerDeliver(message *protobuf.Message) *protobuf.Message {
	switch message.PlDeliver.Message.Type {
	case protobuf.Message_APP_BROADCAST:
		return &protobuf.Message{
			Type:              protobuf.Message_BEB_BROADCAST,
			FromAbstractionId: app.abstraction_id,
			ToAbstractionId:   fmt.Sprintf("%s.beb", app.abstraction_id),
			BebBroadcast: &protobuf.BebBroadcast{
				Message: &protobuf.Message{
					Type:              protobuf.Message_APP_VALUE,
					FromAbstractionId: app.abstraction_id,
					ToAbstractionId:   app.abstraction_id,
					AppValue: &protobuf.AppValue{
						Value: message.PlDeliver.Message.AppBroadcast.Value,
					},
				},
			},
		}
	}

	return nil
}

func (app *App) handleBebLayerDeliver(message *protobuf.Message) *protobuf.Message {
	fmt.Printf("Received value from broadcast: %d\n\n", message.BebDeliver.Message.AppValue.Value.V)

	return &protobuf.Message{
		Type:              protobuf.Message_PL_SEND,
		FromAbstractionId: app.abstraction_id,
		ToAbstractionId:   fmt.Sprintf("%s.pl", app.abstraction_id),
		PlSend: &protobuf.PlSend{
			Message: &protobuf.Message{
				Type:     protobuf.Message_APP_VALUE,
				AppValue: message.BebDeliver.Message.AppValue,
			},
		},
	}
}

func (app *App) HandleMessage(message *protobuf.Message) error {
	var queued_message *protobuf.Message = nil
	fmt.Printf("App handles message:\n%s\n\n", message)

	switch message.Type {
	case protobuf.Message_PL_DELIVER:
		queued_message = app.handlePerfectLinkLayerDeliver(message)
	case protobuf.Message_BEB_DELIVER:
		queued_message = app.handleBebLayerDeliver(message)
	default:
		return errors.New("invalid app message type")
	}

	if queued_message != nil {
		app.messages_queue <- queued_message
	}

	return nil
}

func (app *App) Destroy() {

}
