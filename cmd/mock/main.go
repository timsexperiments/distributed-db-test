package main

import (
	"fmt"

	"github.com/timsexperiments/distributed-db-test/internal/mock"
	"github.com/timsexperiments/distributed-db-test/internal/test"
)

func main() {
	mockDB := &mock.MockDatabase{Multiplier: 10}

	total, group := 1000, 100
	tester := test.NewDbTester(mockDB).WithTotal(total).WithWaitGroup(group)

	totalWriteTime, averageWriteTime := tester.TimeWrites()
	fmt.Printf("Wrote %d rows in %s. Average write time was %s.\n", total, totalWriteTime, averageWriteTime)
	totalReadTime, averageReadTime := tester.TimeReads()
	fmt.Printf("Read %d rows in %s. Average read time was %s.\n", total, totalReadTime, averageReadTime)
}
