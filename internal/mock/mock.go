package mock

import (
	"time"

	"github.com/timsexperiments/distributed-db-test/internal/test"
)

// MockDatabase for demonstration purposes
type MockDatabase struct {
	Multiplier int
}

func (db MockDatabase) ReadTestData(int64) (*test.TestData, error) {
	time.Sleep(time.Duration(db.Multiplier) * time.Microsecond)
	return &test.TestData{}, nil
}

func (db MockDatabase) WriteTestData(test.TestData) error {
	time.Sleep(time.Duration(db.Multiplier) * time.Microsecond)
	return nil
}
