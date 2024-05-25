package consensus

import (
	"time"

	"github.com/EmanuelPutura/distributed_algo/protobuf"
)

func StartTimer(epfd *EventuallyPerfectFailureDetector, delay time.Duration) {
	epfd.timer.Reset(delay)
	go timerCallbackSignalEpfdTimeout(epfd)
}

func timerCallbackSignalEpfdTimeout(epfd *EventuallyPerfectFailureDetector) {
	<-epfd.timer.C
	message := &protobuf.Message{
		Type:              protobuf.Message_EPFD_TIMEOUT,
		FromAbstractionId: epfd.abstraction_id,
		ToAbstractionId:   epfd.abstraction_id,
		EpfdTimeout:       &protobuf.EpfdTimeout{},
	}

	epfd.messages_queue <- message
}
