package command

import (
	"encoding/json"
	"fmt"
	"strings"
)

type Command struct {
	action string
	args   []string
}

func Set(value string, keys ...string) Command {
	return Command{action: "SET", args: append(keys, []string{value}...)}
}

func HSet(hash map[string]string, keys ...string) Command {
	valuesList := make([]string, 0)
	for name, value := range hash {
		valuesList = append(valuesList, name)
		valuesList = append(valuesList, value)
	}
	return Command{action: "HSET", args: append(keys, valuesList...)}
}

func Get(keys ...string) Command {
	return Command{action: "GET", args: keys}
}

func HGet(keys ...string) Command {
	return Command{action: "HGET", args: keys}
}

func HGetAll(keys ...string) Command {
	return Command{action: "HGETALL", args: keys}
}

func Delete(keys ...string) Command {
	return Command{action: "DEL", args: keys}
}

func Custom(action string, args ...string) Command {
	return Command{action: action, args: args}
}

func (command Command) Parts() []string {
	return append([]string{command.action}, command.args...)
}

func (command Command) String() string {
	return fmt.Sprintf("%s %s", command.action, strings.Join(command.args, " "))
}

func (command Command) Json() (string, error) {
	res, err := json.Marshal(append([]string{command.action}, command.args...))
	if err != nil {
		return "", fmt.Errorf("Could not turn command into valid json array: %s", err)
	}
	return string(res), nil
}
