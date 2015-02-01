package interfaces

import (
	"fmt"
	"time"
)

type Reading struct {
	SensorId uint
	Type     string
	Time     time.Time
	Value    float64
}

func (r Reading) String() string {
	return fmt.Sprintf("Reading { SensorId: %v, Type: %q, Time: %q, Value: %v}", r.SensorId, r.Type, r.Time, r.Value)
}
