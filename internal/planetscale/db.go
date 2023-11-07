package planetscale

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/timsexperiments/distributed-db-test/internal/test"
)

type QueryResponse struct {
	Session struct {
		Signature     string `json:"signature"`
		VitessSession struct {
			Autocommit bool `json:"autocommit"`
			Options    struct {
				IncludedFields  string `json:"includedFields"`
				ClientFoundRows bool   `json:"clientFoundRows"`
			} `json:"options"`
			FoundRows            string `json:"foundRows"`
			RowCount             string `json:"rowCount"`
			DDLStrategy          string `json:"DDLStrategy"`
			SessionUUID          string `json:"SessionUUID"`
			EnableSystemSettings bool   `json:"enableSystemSettings"`
		} `json:"vitessSession"`
	} `json:"session"`
	Result struct {
		Fields []struct {
			Name    string `json:"name"`
			Type    string `json:"type"`
			Charset int    `json:"charset"`
			Flags   int    `json:"flags"`
		} `json:"fields"`
		Rows []struct {
			Lengths []string `json:"lengths"`
			Values  string   `json:"values"`
		} `json:"rows"`
	} `json:"result"`
	Timing float64 `json:"timing"`
}

type PlanetScale struct {
	auth string
	url  string
}

func NewPlanetScaleCleint(connectionUrl, auth string) PlanetScale {
	return PlanetScale{auth: auth, url: connectionUrl}
}

func (db PlanetScale) ReadTestData(id int64) (*test.TestData, error) {
	query := fmt.Sprintf("SELECT id, text, timestamp FROM testdata WHERE id = %d;", id)
	response, err := db.Exec(query)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return nil, err
	}
	data, err := extractTestData(*response)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to extract test data from response [%v]: %s\n", response, err)
		return nil, err
	}
	return &data[0], nil
}

func (db PlanetScale) WriteTestData(data test.TestData) error {
	insertQuery := fmt.Sprintf("INSERT INTO testdata (id, text, timestamp) VALUES (%d, '%s', '%s')", data.Key, data.Text, data.Timestamp.Format("2006-01-02 15:04:05.999999"))
	_, err := db.Exec(insertQuery)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		return err
	}

	return nil
}

func (db PlanetScale) Exec(sql string) (*QueryResponse, error) {
	// i love you stupid and nerd head, you silly cutie
	url := db.url
	method := "POST"

	payload := strings.NewReader(fmt.Sprintf(`{
    "query": "%s",
    "session": null
}`, sql))
	client := &http.Client{}
	req, err := http.NewRequest(method, url, payload)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", fmt.Sprintf("Basic %s", os.Getenv("PLANETSCALE_AUTH")))

	res, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		return nil, err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		return nil, err
	}

	var response QueryResponse
	err = json.Unmarshal(body, &response)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		return nil, err
	}
	return &response, nil
}

func extractTestData(response QueryResponse) ([]test.TestData, error) {
	parsed, err := parseRows(response)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to parse response rows: %s.\n", err)
		return nil, err
	}
	data := make([]test.TestData, len(parsed))
	for i, parsedRowData := range parsed {
		data[i] = rowToTestData(parsedRowData)
	}
	return data, nil
}

func rowToTestData(data map[string]any) test.TestData {
	key := int64(data["id"].(int))
	text := data["text"].(string)
	timestamp := data["timestamp"].(time.Time)
	return test.TestData{Key: key, Text: text, Timestamp: timestamp}
}

func parseRows(response QueryResponse) ([]map[string]any, error) {
	fields := response.Result.Fields
	rows := response.Result.Rows

	result := make([]map[string]any, len(rows))
	for i, row := range rows {
		currentValueIndex := 0
		rowValue := map[string]any{}
		encoded := row.Values
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, err
		}

		for column := 0; column < len(fields); column++ {
			sqlType := fields[column].Type
			name := fields[column].Name
			length, err := strconv.Atoi(row.Lengths[column])
			if err != nil {
				return nil, err
			}
			value := decoded[currentValueIndex : currentValueIndex+length]
			currentValueIndex += length
			goValue, err := convertValue(sqlType, string(value))
			if err != nil {
				return nil, err
			}
			rowValue[name] = goValue
		}
		if i < len(rows) {
			result[i] = rowValue
		}
	}

	return result, nil
}

func convertValue(sqlType, value string) (any, error) {
	switch sqlType {
	case "INT32":
		return strconv.Atoi(value)
	case "VARCHAR":
		return value, nil
	case "DATETIME":
		return time.Parse("2006-01-02 15:04:05", value)
	}
	return nil, fmt.Errorf("No type found for %s.", sqlType)
}
