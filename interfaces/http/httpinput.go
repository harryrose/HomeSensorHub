package httpinput

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/ant0ine/go-json-rest/rest"
	"github.com/harryrose/sensor-mediator/interfaces"
)

func NewHttpInput(port uint) interfaces.Input {
	return &httpInput{port, rest.ResourceHandler{}}
}

type httpInput struct {
	port    uint
	handler rest.ResourceHandler
}

func (*httpInput) GetName() string {
	return "HttpInput"
}

type value struct {
	Value float64 `json:"value"`
}

func (h *httpInput) Begin(outputChannel chan<- interfaces.Reading, endChannel <-chan interface{}) {
	handlePost := func(w rest.ResponseWriter, req *rest.Request) {
		var v value
		err := req.DecodeJsonPayload(&v)

		if err != nil {
			rest.Error(w, err.Error(), 400)
			return
		}

		sensorIdString := req.PathParam("sensorId")
		sensorIdInt, err := strconv.Atoi(sensorIdString)
		if err != nil {
			rest.Error(w, "Invalid SensorId", 400)
			log.Error("Non-integer sensorId")
			return
		}

		if sensorIdInt < 0 {
			rest.Error(w, "Invalid SensorId", 400)
			log.Error("Non-positive sensorId")
			return
		}

		valueType := strings.TrimSpace(req.PathParam("type"))
		if valueType == "" {
			rest.Error(w, "Invalid Type", 400)
			log.Error("Invalid type")
			return
		}

		outputChannel <- interfaces.Reading{
			SensorId: uint(sensorIdInt),
			Type:     valueType,
			Value:    v.Value,
		}

		w.WriteHeader(http.StatusOK)
	}

	err := h.handler.SetRoutes(
		&rest.Route{"POST", "/:sensorId/:type", handlePost},
	)

	if err != nil {
		log.WithField("error", err).Error("Error setting routes")
		return
	}

	portString := fmt.Sprintf(":%d", h.port)
	log.WithField("Port", portString).Info("Listening for http")
	log.Error(http.ListenAndServe(portString, &h.handler))
}
