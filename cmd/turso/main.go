package main

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/libsql/libsql-client-go/libsql"
	"github.com/timsexperiments/distributed-db-test/internal/test"
	"github.com/timsexperiments/distributed-db-test/internal/turso"
)

func main() {
	config := getConfig()
	var dbUrl = config.url
	db, err := sql.Open("libsql", dbUrl)

	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open db %s: %s", dbUrl, err)
		os.Exit(1)
	}
	db.Exec("DROP TABLE IF EXISTS testdata")
	db.Exec("CREATE TABLE IF NOT EXISTS testdata (key INT PRIMARY KEY, text VARCHAR(255), timestamp DATETIME)")

	turso := &turso.Turso{Db: db}
	total, group, pause := 1000, 100, time.Duration(10)*time.Second

	tester := test.NewDbTester(turso).WithTotal(total).WithPause(pause).WithWaitGroup(group)
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
	return config{url: os.Getenv("TURSO_URL")}
}

type config struct {
	url string
}
