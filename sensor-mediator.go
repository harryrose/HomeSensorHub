package main

import (
	"errors"
	"os"
	"strings"
	"time"

	"code.google.com/p/gcfg"

	log "github.com/Sirupsen/logrus"
	"github.com/harryrose/sensor-mediator/interfaces"
	"github.com/harryrose/sensor-mediator/interfaces/debug"
	"github.com/harryrose/sensor-mediator/interfaces/http"
	"github.com/harryrose/sensor-mediator/interfaces/mongo"
	"github.com/harryrose/sensor-mediator/interfaces/zmq"
)

type Config struct {
	Mongo struct {
		Server     string
		Port       uint
		Database   string
		Collection string
		Username   string
		Password   string
	}
	ZmqOut struct {
		Port uint
	}
	ZmqIn struct {
		Port uint
	}
	HttpIn struct {
		Port uint
	}
	Log struct {
		Level string
		File  string
	}
}

func main() {
	var config Config
	if err := gcfg.ReadFileInto(&config, "config.gcfg"); err != nil {
		log.WithField("error", err).Fatal("Could not read configuration file")
	}

	level, err := log.ParseLevel(config.Log.Level)

	if err != nil {
		if config.Log.Level != "" {
			log.WithFields(log.Fields{"level": config.Log.Level, "error": err}).Warn("Invalid log level")
		}
	} else {
		log.SetLevel(level)
	}

	if config.Log.File != "" {

		logFile, err := os.OpenFile(config.Log.File, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			log.WithFields(log.Fields{"error": err, "file": config.Log.File}).Fatal("Could not open log file")
		}
		log.SetOutput(logFile)
		defer logFile.Close()
	}

	inputs := []interfaces.Input{
		//	debug.NewTimedInput(1),
		zeromq3.NewZeroMQ3Input(config.ZmqIn.Port),
		httpinput.NewHttpInput(config.HttpIn.Port),
	}

	outputs := []interfaces.Output{
		debug.NewPrintOutput(),
		mongo.NewMongoOutput(config.Mongo.Server, config.Mongo.Port, config.Mongo.Username, config.Mongo.Password, config.Mongo.Database, config.Mongo.Collection),
		zeromq3.NewZeroMQ3Output(config.ZmqOut.Port),
	}

	inputChannel := make(chan interfaces.Reading)
	endChannel := make(chan interface{})
	outputChannels := make([]chan interfaces.Reading, len(outputs))

	for i, o := range outputs {
		log.WithField("output", o.GetName()).Info("Starting output")
		outputChannels[i] = make(chan interfaces.Reading)
		go o.Begin(outputChannels[i], endChannel)
	}

	for _, i := range inputs {
		log.WithField("input", i.GetName()).Info("Starting input")
		go i.Begin(inputChannel, endChannel)
	}

	listen(inputChannel, outputChannels, outputs, endChannel)
}

func sanitise(reading *interfaces.Reading) error {

	var defaultTime time.Time

	if reading.SensorId < 0 {
		return errors.New("Missing SensorId")
	}

	reading.Type = strings.TrimSpace(reading.Type)

	if reading.Type == "" {
		return errors.New("Missing Type")
	}

	if reading.Time == defaultTime {
		reading.Time = time.Now().UTC()
	}

	return nil
}

func broadcast(reading *interfaces.Reading, outputChannels []chan interfaces.Reading, outputs []interfaces.Output) {
	log.Debug("Beginning broadcast")

	for i, o := range outputChannels {
		log.WithFields(log.Fields{"output": outputs[i].GetName(), "reading": reading}).Debug("Sending output")
		o <- *reading
		log.WithFields(log.Fields{"output": outputs[i].GetName(), "reading": reading}).Debug("Sent output")
	}
	log.Debug("Finished broadcast")
}

func listen(inputChannel chan interfaces.Reading, outputChannels []chan interfaces.Reading, outputs []interfaces.Output, endChannel chan interface{}) {
	keepGoing := true
	for keepGoing {
		log.Debug("Waiting for input")
		select {
		case <-endChannel:
			keepGoing = false

		case input := <-inputChannel:
			log.WithField("reading", input).Debug("Input received")
			err := sanitise(&input)

			if err != nil {
				log.WithFields(log.Fields{"reading": input, "error": err.Error()}).Warn("Failed sanitisation")
			} else {
				broadcast(&input, outputChannels, outputs)
			}
		}
	}
}
