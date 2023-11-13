package main

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
	"github.com/timsexperiments/distributed-db-test/internal/test"
	"github.com/timsexperiments/distributed-db-test/internal/upstash"
)

func main() {
	config := getConfig()

	db := upstash.NewUpstashClient(config.url, config.token)
	db.Clean()

	total, group := 1000, 100
	tester := test.NewDbTester(db).WithTotal(total).WithWaitGroup(group)
	writeTotal, writeAverage := tester.TimeWrites()
	fmt.Printf("Wrote %d records in %s. Average write time was %s.\n", total, writeTotal, writeAverage)
	readTotal, readAverage := tester.TimeReads()
	fmt.Printf("Read %d records in %s. Average read time was %s.\n", total, readTotal, readAverage)
}

func getConfig() config {
	err := godotenv.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to read environment variables: %s", err)
		os.Exit(1)
	}
	return config{url: os.Getenv("UPSTASH_REDIS_URL"), token: os.Getenv("UPSTASH_REDIS_TOKEN")}
}

type config struct {
	url   string
	token string
}
