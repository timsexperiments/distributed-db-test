package turso

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/timsexperiments/distributed-db-test/internal/test"
)

type Turso struct {
	Db *sql.DB
}

func (turso Turso) ReadTestData(key int64) (*test.TestData, error) {
	rows, err := turso.Db.Query("SELECT key, text, timestamp FROM testdata WHERE key = ?", key)
	if err != nil {
		fmt.Fprintf(os.Stderr, "There was an error reading testdata [%d] from the database: %v\n", key, err.Error())
		return nil, err
	}
	defer rows.Close()
	if rows.Next() {
		result := &test.TestData{}
		rows.Scan(&result.Key, &result.Text, &result.Timestamp)
		return result, nil
	}
	return nil, nil
}

func (turso Turso) WriteTestData(data test.TestData) error {
	_, err := turso.Db.Exec("INSERT INTO testdata(key, text, timestamp) VALUES (?, ?, ?)", data.Key, data.Text, data.Timestamp)
	if err != nil {
		fmt.Fprintf(os.Stderr, "There was an error writing testdata [%v] to the database: %v\n", data, err.Error())
	}
	return err
}
