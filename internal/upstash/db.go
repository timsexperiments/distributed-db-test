package upstash

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/timsexperiments/distributed-db-test/internal/test"
	"github.com/timsexperiments/distributed-db-test/internal/upstash/command"
)

type Upstash struct {
	url   string
	token string
}

func NewUpstashClient(url, token string) Upstash {
	return Upstash{url: url, token: token}
}

func (db Upstash) ReadTestData(key int64) (*test.TestData, error) {
	lookupKey := fmt.Sprintf("testdata:%d", key)
	res, err := db.request(command.HGet(lookupKey, "key"), command.HGet(lookupKey, "text"), command.HGet(lookupKey, "timestamp"))
	if err != nil {
		return nil, err
	}
	var responses []Response
	err = json.Unmarshal(res, &responses)
	if err != nil {
		return nil, err
	}
	if (len(responses)) != 3 {
		return nil, fmt.Errorf("There should have been 3 results. Found [%d].", len(responses))
	}
	foundKey, err := strconv.ParseInt(responses[0].Result, 10, 64)
	if err != nil {
		return nil, err
	}
	foundText := responses[1].Result
	foundTimestamp, err := strconv.ParseInt(responses[2].Result, 10, 64)
	if err != nil {
		return nil, err
	}
	return &test.TestData{Key: foundKey, Text: foundText, Timestamp: time.Unix(0, foundTimestamp)}, nil
}

func (db Upstash) WriteTestData(data test.TestData) error {
	dataMap := make(map[string]string)
	dataMap["key"] = fmt.Sprint(data.Key)
	dataMap["text"] = data.Text
	dataMap["timestamp"] = fmt.Sprint(data.Timestamp.UnixNano())
	command := command.HSet(dataMap, fmt.Sprintf("testdata:%d", data.Key))
	_, err := db.request(command)
	if err != nil {
		return err
	}
	return nil
}

func (db Upstash) Clean() error {
	_, err := db.request(command.Custom("FLUSHALL"))
	if err != nil {
		return err
	}
	return nil
}

type Response struct {
	Result string `json:"result"`
}

func (db Upstash) request(commands ...command.Command) ([]byte, error) {
	requestUrl, err := url.JoinPath(db.url, "pipeline")
	if err != nil {
		return nil, err
	}
	client := &http.Client{}

	commandsList := make([]string, 0)
	for _, command := range commands {
		commandJson, err := command.Json()
		if err != nil {
			return nil, err
		}
		commandsList = append(commandsList, commandJson)
	}
	payload := strings.NewReader(fmt.Sprintf("[%s]", strings.Join(commandsList, ",")))

	req, err := http.NewRequest("POST", requestUrl, payload)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", db.token))

	// fmt.Printf("Request:\n\turl: %s\n\tmethod: %s\n\tbody: %s\n", req.URL, req.Method, commandsList)
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
	// fmt.Printf("Response: %s\n", string(body))
	return body, nil
}
