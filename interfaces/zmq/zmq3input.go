package zeromq3

import (
	"encoding/json"
	"fmt"

	log "github.com/Sirupsen/logrus"
	"github.com/harryrose/sensor-mediator/interfaces"
	"github.com/pebbe/zmq3"
)

func NewZeroMQ3Input(port uint) interfaces.Input {
	return &zmq3input{port}
}

type zmq3input struct {
	port uint
}

func (*zmq3input) GetName() string {
	return "ZeroMQ3Input"
}

func (z *zmq3input) logError(err error, msg string) {
	log.WithFields(log.Fields{"input": z.GetName(), "error": err}).Error(msg)
}

func (z *zmq3input) Begin(outputChannel chan<- interfaces.Reading, endChannel <-chan interface{}) {
	socket, err := zmq3.NewSocket(zmq3.SUB)

	if err != nil {
		z.logError(err, "Could not create socket")
		return
	}

	bindAddr := fmt.Sprintf("tcp://*:%d", z.port)
	log.Infof("Bind address: %s", bindAddr)

	err = socket.Bind(bindAddr)

	if err != nil {
		z.logError(err, "Could not bind")
		return
	}

	socket.SetSubscribe("")

	for {
		message, err := socket.Recv(0)
		if err != nil {
			z.logError(err, "Error on receipt")
			continue
		}

		log.WithFields(log.Fields{"input": z.GetName(), message: message}).Debug("Message Received")

		var reading interfaces.Reading
		err = json.Unmarshal([]byte(message), &reading)

		if err != nil {
			log.WithFields(log.Fields{"input": z.GetName(), message: message, "error": err}).Error("Cold not unmarshal message")
			continue
		}

		outputChannel <- reading
	}

}
