package test

import (
	"fmt"
	"math"
	"os"
	"sync"
	"time"
)

// Test data object used to run the Tester read and write tests.
type TestData struct {
	Key       int64
	Timestamp time.Time
	Text      string
}

func (data TestData) String() string {
	return fmt.Sprintf("{ Key: %d, Text: %s, Timestamp: %s }", data.Key, data.Text, data.Timestamp)
}

// A test database that writes and reads test data.
type TestDatabase interface {
	WriteTestData(TestData) error          // Writes test data to the database.
	ReadTestData(int64) (*TestData, error) // Reads test data from the database.
}

const testTotalWriteDefault = 1000
const testWaitGroupDefault = 100
const testPauseTimeDefault = 0

// A tester object used to run distributed database tests for reads and writes.
type dbTester struct {
	db        TestDatabase  // The database to run the tests on. This is required.
	total     int           // The total number of requests that should be sent.
	waitGroup int           // The number of concurrent requests to send at a time.
	pause     time.Duration // The time to pause between each wait group.
	verbose   bool          // Whether to log extra info.
}

// Writes the total amount of test data to the test database
func (tester *dbTester) TimeWrites() (time.Duration, time.Duration) {
	total, waitGroup, pause := tester.total, tester.waitGroup, tester.pause
	var writeMutex sync.Mutex

	var wg sync.WaitGroup
	writeTimes := make([]time.Duration, 0)
	for i := 1; i <= int(math.Ceil(float64(total)/float64(waitGroup))); i++ {
		for j := 1; j <= waitGroup; j++ {
			key := (i-1)*waitGroup + j
			wg.Add(1)
			data := TestData{
				Key:       int64(key),
				Timestamp: time.Now().Add(time.Duration(key) * time.Second),
				Text:      fmt.Sprintf("SampleText-%d", key),
			}
			go func(data TestData) {
				if tester.verbose {
					fmt.Printf("Started writing %d.\n", key)
				}
				defer wg.Done()
				writeStart := time.Now()
				err := tester.db.WriteTestData(data)
				writeDuration := time.Since(writeStart)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Unable to write test data: %s\n", err.Error())
					os.Exit(1)
				}
				if tester.verbose {
					fmt.Printf("Finished writing %d in %s.\n", key, writeDuration)
				}

				writeMutex.Lock()
				writeTimes = append(writeTimes, writeDuration)
				writeMutex.Unlock()
			}(data)
		}
		wg.Wait()
		if tester.verbose {
			fmt.Printf("Wrote %d rows.\n", waitGroup)
			fmt.Printf("Waiting for %s.\n", pause)
		}
		time.Sleep(pause)
	}

	if tester.verbose {
		fmt.Printf("Done writing %d rows.\n", total)
	}

	totalTime := sum(writeTimes)
	averageTime := float64(totalTime) / float64(len(writeTimes))
	return time.Duration(totalTime), time.Duration(averageTime)
}

// Writes a set of test data.
func (tester dbTester) TimeReads() (time.Duration, time.Duration) {
	total, waitGroup, pause := tester.total, tester.waitGroup, tester.pause
	var readMutex sync.Mutex

	var wg sync.WaitGroup
	readTimes := make([]time.Duration, 0)
	for i := 1; i <= int(math.Ceil(float64(total)/float64(waitGroup))); i++ {
		for j := 1; j <= waitGroup; j++ {
			key := (i-1)*waitGroup + j
			wg.Add(1)
			go func(key int64) {
				if tester.verbose {
					fmt.Printf("Started reading %d.\n", key)
				}
				defer wg.Done()
				readStart := time.Now()
				data, err := tester.db.ReadTestData(key)
				readDuration := time.Since(readStart)
				if tester.verbose {
					fmt.Printf("Finished reading { key = %d, text = %s, time = %v } in %s.\n", data.Key, data.Text, data.Timestamp, readDuration)
				}
				if err != nil {
					fmt.Fprintf(os.Stderr, "Unable to read test data: %s\n", err.Error())
					os.Exit(1)
				}

				readMutex.Lock()
				readTimes = append(readTimes, readDuration)
				readMutex.Unlock()
			}(int64(key))
		}
		wg.Wait()
		if tester.verbose {
			fmt.Printf("Read %d rows.\n", waitGroup)
			fmt.Printf("Waiting for %s.\n", pause)
		}
		time.Sleep(pause)
	}

	if tester.verbose {
		fmt.Printf("Done reading %d rows.\n", total)
	}

	totalTime := sum(readTimes)
	averageTime := float64(totalTime) / float64(len(readTimes))
	return time.Duration(totalTime), time.Duration(averageTime)
}

// Creates new database tester using the given database.
func NewDbTester(db TestDatabase) (tester dbTester) {
	tester.db = db
	tester.total = testTotalWriteDefault
	tester.pause = testPauseTimeDefault
	tester.waitGroup = testWaitGroupDefault
	tester.verbose = false
	return
}

// Sets the total size of test data to read and write.
func (tester dbTester) WithTotal(total int) dbTester {
	tester.total = total
	return tester
}

// Sets the pause duration between each wait group.
func (tester dbTester) WithPause(pause time.Duration) dbTester {
	tester.pause = pause
	return tester
}

// Sets the size of each test group.
func (tester dbTester) WithWaitGroup(waitGroup int) dbTester {
	tester.waitGroup = waitGroup
	return tester
}

// Turns on verbose mode.
func (tester dbTester) WithVerbose() dbTester {
	tester.verbose = true
	return tester
}

// Turns off verbose mode.
func (tester dbTester) WithoutVerbose() dbTester {
	tester.verbose = false
	return tester
}

// Sums the durations in the slice and returns the total nanoseconds.
func sum(data []time.Duration) (result int64) {
	result = 0
	for _, duration := range data {
		result += duration.Nanoseconds()
	}
	return
}
