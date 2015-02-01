package debug

import (
	"github.com/Sirupsen/logrus"
	"github.com/harryrose/sensor-mediator/interfaces"
)

func NewPrintOutput() interfaces.Output {
	return &printOutput{}
}

type printOutput struct{}

func (*printOutput) GetName() string {
	return "PrintOutput"
}

func (*printOutput) Begin(inputChannel <-chan interfaces.Reading, endChannel <-chan interface{}) {
	keepGoing := true
	for keepGoing {
		select {
		case <-endChannel:
			logrus.Debug("Ending output")
			keepGoing = false

		case reading := <-inputChannel:
			logrus.WithField("reading", reading).Debug("Output")
		}
	}
}
