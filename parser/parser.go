package parser

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/srvc/fail"
)

type Paraser interface {
	Parse(ctx context.Context, command string) (string, string, map[string]string, error)
}

type parserImp struct {
}

func NewParser() Paraser {
	return &parserImp{}
}

func (p *parserImp) Parse(ctx context.Context, command string) (string, string, map[string]string, error) {
	var operation string
	var target string
	var argument map[string]string

	if p == nil {
		return operation, target, argument, fail.New("Un initialized parser executed")
	}

	commands := strings.Split(command, " ")
	if len(commands) < 2 {
		return operation, target, argument, fail.New("Not enough args")
	}

	operation = commands[0]
	target = commands[1]

	commands = commands[2:]

	for i, command := range commands {
		if strings.HasPrefix(command, "-") {
			keyAndValue := strings.Split(command, "=")
			if len(keyAndValue) != 2 {
				argument[strconv.Itoa(i)] = command
			} else if len(keyAndValue) > 2 {
				return operation, target, argument, fail.New(fmt.Sprintf("Failed to parse %s", command))
			} else {
				argument[keyAndValue[0]] = keyAndValue[1]
			}
		}

		argument[]
	}

	return operation, target,
}
