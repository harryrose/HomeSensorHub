package mongo

import (
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/harryrose/sensor-mediator/interfaces"
	"gopkg.in/mgo.v2"
)

// Constructors

func NewMongoOutput(server string, port uint, username string, password string, database string, collection string) interfaces.Output {
	return &mongoOutput{
		server,
		port,
		username,
		password,
		database,
		collection,
		nil,
	}
}

// Private structures and Interface methods

type mongoOutput struct {
	server     string
	port       uint
	username   string
	password   string
	database   string
	collection string
	session    *mgo.Session
}

func (m *mongoOutput) String() string {
	return fmt.Sprintf("MongoOutput { server: %q, port: %v, username:%q, password: ***, database: %q, collection: %q}",
		m.server,
		m.port,
		m.username,
		m.database,
		m.collection)
}

type mongoReadingId struct {
	Time   time.Time `bson:"time"`
	Sensor int64     `bson:"sensor"`
	Type   string    `bson:"type"`
}

type mongoReading struct {
	Id    mongoReadingId `bson:"_id"`
	Value float64        `bson:"value"`
}

func (m *mongoOutput) GetName() string {
	return "MongoDB"
}

func (m *mongoOutput) Begin(inputChannel <-chan interfaces.Reading, endChannel <-chan interface{}) {
	keepGoing := true
	for keepGoing {
		select {
		case reading := <-inputChannel:
			m.handleReading(&reading)

		case <-endChannel:
			keepGoing = false
		}
	}
}

// Private methods

func (m *mongoOutput) logError(err error, message string) {
	log.WithFields(log.Fields{"mongoOutput": m, "error": err}).Error(message)
}

func (m *mongoOutput) handleReading(reading *interfaces.Reading) {
	log.Debug("Inserting reading into mongodb")
	if err := m.ensureMongoConnection(); err != nil {
		m.logError(err, "Unable to start mongo session")
		return
	}

	collection := m.session.DB(m.database).C(m.collection)

	if err := collection.Insert(&mongoReading{mongoReadingId{reading.Time, int64(reading.SensorId), reading.Type}, reading.Value}); err != nil {
		m.logError(err, "Unable to start mongo session")
	} else {
		log.Debug("Added row to mongodb")
	}
}

func (m *mongoOutput) ensureMongoConnection() error {
	if m.session == nil {
		session, err := m.connectToMongo()
		if err != nil {
			return err
		}

		m.session = session
	}

	return nil
}

func (m *mongoOutput) connectToMongo() (*mgo.Session, error) {
	usernamePasswordCombo := ""
	portPart := ""

	if m.username != "" {
		usernamePasswordCombo = m.username + ":" + m.password + "@"
	}

	if m.port != 0 {
		portPart = ":" + string(m.port)
	}

	connectionUri := usernamePasswordCombo + m.server + portPart

	return mgo.Dial(connectionUri)
}
