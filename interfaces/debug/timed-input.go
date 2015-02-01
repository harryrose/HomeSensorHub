package debug

import (
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/harryrose/sensor-mediator/interfaces"
)

func NewTimedInput(periodSeconds uint) interfaces.Input {
	if periodSeconds == 0 {
		periodSeconds = 1
	}
	return &timedInput{periodSeconds}
}

type timedInput struct {
	periodSeconds uint
}

func (t *timedInput) GetName() string {
	return fmt.Sprintf("TimedInput (%v)", t.periodSeconds)
}

func (*timedInput) trigger(outputChannel chan<- interfaces.Reading) {
	log.Debug("Sending")
	outputChannel <- interfaces.Reading{
		999,
		"Test",
		time.Now().UTC(),
		123,
	}
}

func (t *timedInput) Begin(outputChannel chan<- interfaces.Reading, endChannel <-chan interface{}) {
	keepGoing := true

	timer := func(trigger chan interface{}) {
		for {
			log.Debug("Tick")
			trigger <- 1
			time.Sleep(time.Second * time.Duration(t.periodSeconds))
		}
	}

	timerChannel := make(chan interface{})

	go timer(timerChannel)

	for keepGoing {
		select {
		case <-timerChannel:
			t.trigger(outputChannel)

		case <-endChannel:
			keepGoing = false
		}
	}
}
