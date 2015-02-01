package zeromq3

import (
	"encoding/json"
	"fmt"

	log "github.com/Sirupsen/logrus"
	"github.com/harryrose/sensor-mediator/interfaces"
	"github.com/pebbe/zmq3"
)

func NewZeroMQ3Output(portNumber uint) interfaces.Output {
	return &zeroMqOutput{portNumber, nil}
}

type zeroMqOutput struct {
	portNumber uint
	socket     *zmq3.Socket
}

func (z *zeroMqOutput) GetName() string {
	return "ZeroMQ3Output"
}

func (z *zeroMqOutput) Begin(inputChannel <-chan interfaces.Reading, endChannel <-chan interface{}) {
	defer z.disconnectSocket()

	keepGoing := true
	for keepGoing {
		select {
		case <-endChannel:
			keepGoing = false

		case reading := <-inputChannel:
			if err := z.handleReading(&reading); err != nil {
				z.logError(err, "Error processing input")
			}
		}
	}
}

func (z *zeroMqOutput) disconnectSocket() {
	if z.socket != nil {
		z.socket.Close()
	}
}

func (z *zeroMqOutput) handleReading(reading *interfaces.Reading) error {
	if err := z.ensureSocket(); err != nil {
		return err
	}

	toSend, err := json.Marshal(reading)
	if err != nil {
		return err
	}

	_, err = z.socket.Send(fmt.Sprint(reading.SensorId), zmq3.SNDMORE)
	if err != nil {
		return err
	}

	_, err = z.socket.SendBytes(toSend, 0)
	return err
}

func (z *zeroMqOutput) logError(err error, msg string) {
	log.WithFields(log.Fields{"output": z.GetName(), "error": err}).Error(msg)
}

func (z *zeroMqOutput) ensureSocket() error {
	if z.socket == nil {
		log.Debug("Opening ZMQ Publish socket")
		if socket, err := zmq3.NewSocket(zmq3.PUB); err != nil {
			return err
		} else {
			z.socket = socket
		}

		bindAddress := fmt.Sprintf("tcp://*:%d", z.portNumber)

		log.WithField("address", bindAddress).Info("Binding ZMQ publish socket")
		err := z.socket.Bind(bindAddress)
		return err
	}
	return nil
}
