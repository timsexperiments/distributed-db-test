package main

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
	"github.com/timsexperiments/distributed-db-test/internal/planetscale"
	"github.com/timsexperiments/distributed-db-test/internal/test"
)

func main() {
	config := getConfig()

	db := planetscale.NewPlanetScaleCleint(config.url, config.auth)
	db.Exec("DROP TABLE IF EXISTS testdata")
	db.Exec("CREATE TABLE IF NOT EXISTS testdata (id INT PRIMARY KEY, text VARCHAR(255), timestamp DATETIME)")

	total, group := 1000, 100
	tester := test.NewDbTester(db).WithTotal(total).WithWaitGroup(group)
	writeTotal, writeAverage := tester.TimeWrites()
	fmt.Printf("Wrote %d rows in %s. Average write time was %s.\n", total, writeTotal, writeAverage)
	readTotal, readAverage := tester.TimeReads()
	fmt.Printf("Read %d rows in %s. Average read time was %s.\n", total, readTotal, readAverage)
}

func getConfig() config {
	err := godotenv.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to read environment variables: %s", err)
		os.Exit(1)
	}
	return config{auth: os.Getenv("PLANETSCALE_AUTH"), url: os.Getenv("PLANETSCALE_DB_URL")}
}

type config struct {
	url  string
	auth string
}
