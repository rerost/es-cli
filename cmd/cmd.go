package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/rerost/es-cli/config"
	"github.com/srvc/fail"
	"go.uber.org/zap"
)

func Run() error {
	ctx := context.TODO()

	cmd, err := InitializeCmd(ctx, config.Config{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create cmd: %v\n", err)
	}

	l, err := InitializeLogger(config.Config{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create logger: %v\n", err)
	}
	defer l.Sync()

	zap.ReplaceGlobals(l)
	return fail.Wrap(cmd.Execute())
}
