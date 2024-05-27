package app

import (
	"errors"
	"fmt"

	"github.com/EmanuelPutura/distributed_algo/helpers"
	dlog "github.com/EmanuelPutura/distributed_algo/log"
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
	case protobuf.Message_APP_VALUE:
		return &protobuf.Message{
			Type:              protobuf.Message_PL_SEND,
			FromAbstractionId: "app",
			ToAbstractionId:   "app.pl",
			PlSend: &protobuf.PlSend{
				Message: &protobuf.Message{
					Type:     protobuf.Message_APP_VALUE,
					AppValue: message.PlDeliver.Message.AppValue,
				},
			},
		}
	case protobuf.Message_APP_WRITE:
		return &protobuf.Message{
			Type:              protobuf.Message_NNAR_WRITE,
			FromAbstractionId: "app",
			ToAbstractionId:   fmt.Sprintf("app.nnar[%s]", message.PlDeliver.Message.AppWrite.Register),
			NnarWrite: &protobuf.NnarWrite{
				Value: message.PlDeliver.Message.AppWrite.Value,
			},
		}
	case protobuf.Message_APP_READ:
		return &protobuf.Message{
			Type:              protobuf.Message_NNAR_READ,
			FromAbstractionId: "app",
			ToAbstractionId:   fmt.Sprintf("app.nnar[%s]", message.PlDeliver.Message.AppRead.Register),
			NnarRead:          &protobuf.NnarRead{},
		}
	case protobuf.Message_APP_PROPOSE:
		return &protobuf.Message{
			Type:              protobuf.Message_UC_PROPOSE,
			FromAbstractionId: "app",
			ToAbstractionId:   fmt.Sprintf("app.uc[%s]", message.PlDeliver.Message.AppPropose.Topic),
			UcPropose: &protobuf.UcPropose{
				Value: message.PlDeliver.Message.AppPropose.Value,
			},
		}
	}

	return nil
}

func (app *App) handleBebLayerDeliver(message *protobuf.Message) *protobuf.Message {
	// fmt.Printf("Received value from broadcast: %d\n\n", message.BebDeliver.Message.AppValue.Value.V)
	dlog.Dlog.Printf("Received value from broadcast: %d\n\n", message.BebDeliver.Message.AppValue.Value.V)

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

func (app *App) handleNnarLayerWriteReturn(message *protobuf.Message) *protobuf.Message {
	return &protobuf.Message{
		Type:              protobuf.Message_PL_SEND,
		FromAbstractionId: "app",
		ToAbstractionId:   "app.pl",
		PlSend: &protobuf.PlSend{
			Message: &protobuf.Message{
				Type: protobuf.Message_APP_WRITE_RETURN,
				AppWriteReturn: &protobuf.AppWriteReturn{
					Register: helpers.RetrieveIdFromAbstraction(message.FromAbstractionId),
				},
			},
		},
	}
}

func (app *App) handleNnarLayerReadReturn(message *protobuf.Message) *protobuf.Message {
	return &protobuf.Message{
		Type:              protobuf.Message_PL_SEND,
		FromAbstractionId: "app",
		ToAbstractionId:   "app.pl",
		PlSend: &protobuf.PlSend{
			Message: &protobuf.Message{
				Type: protobuf.Message_APP_READ_RETURN,
				AppReadReturn: &protobuf.AppReadReturn{
					Register: helpers.RetrieveIdFromAbstraction(message.FromAbstractionId),
					Value:    message.NnarReadReturn.Value,
				},
			},
		},
	}
}

func (app *App) handleUcLayerDecide(message *protobuf.Message) *protobuf.Message {
	return &protobuf.Message{
		Type:              protobuf.Message_PL_SEND,
		FromAbstractionId: "app",
		ToAbstractionId:   "app.pl",
		PlSend: &protobuf.PlSend{
			Message: &protobuf.Message{
				Type:            protobuf.Message_APP_DECIDE,
				ToAbstractionId: "app",
				AppDecide: &protobuf.AppDecide{
					Value: message.UcDecide.Value,
				},
			},
		},
	}
}

func (app *App) HandleMessage(message *protobuf.Message) error {
	// fmt.Printf("App handles message:\n%s\n\n", message)
	dlog.Dlog.Printf("%-35s App handles message:\n'%s'\n\n", "[app]:", message)
	var queued_message *protobuf.Message = nil

	switch message.Type {
	case protobuf.Message_PL_DELIVER:
		queued_message = app.handlePerfectLinkLayerDeliver(message)
	case protobuf.Message_BEB_DELIVER:
		queued_message = app.handleBebLayerDeliver(message)
	case protobuf.Message_NNAR_READ_RETURN:
		queued_message = app.handleNnarLayerReadReturn(message)
	case protobuf.Message_NNAR_WRITE_RETURN:
		queued_message = app.handleNnarLayerWriteReturn(message)
	case protobuf.Message_UC_DECIDE:
		queued_message = app.handleUcLayerDecide(message)
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
