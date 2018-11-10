package main

import (
	"context"
	"fmt"
	"os"

	"github.com/rerost/es-cli/executer"
	"github.com/rerost/es-cli/setting"
	"github.com/srvc/fail"
	"github.com/urfave/cli"
)

func main() {
	app := cli.NewApp()

	app.Name = "es-cli"
	app.Usage = "TODO"

	app.Action = func(cliContext *cli.Context) error {
		ctx := context.Background()
		head := cliContext.Args().First()
		args := cliContext.Args().Tail()

		if head == "" {
			return fail.New("You need <operation>")
		}
		operation := head

		head = cli.Args(args).First()
		args = cli.Args(args).Tail()
		if head == "" {
			return fail.New("You need <target>")
		}
		target := head

		ctx = context.WithValue(ctx, setting.SettingKey("host"), cliContext.String("host"))
		ctx = context.WithValue(ctx, setting.SettingKey("port"), cliContext.String("port"))
		ctx = context.WithValue(ctx, setting.SettingKey("basic-user"), cliContext.String("user"))
		ctx = context.WithValue(ctx, setting.SettingKey("basic-pass"), cliContext.String("pass"))

		e := executer.NewExecuter()
		result, err := e.Run(ctx, operation, target, args)
		fmt.Fprintf(os.Stdout, result.String())
		return err
	}
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "host",
			Usage: "ES hostname",
		},
		cli.StringFlag{
			Name:  "port, p",
			Usage: "ES port",
		},
		cli.StringFlag{
			Name:  "user, U",
			Usage: "ES basic auth user",
		},
		cli.StringFlag{
			Name:  "password, P",
			Usage: "ES basic auth password",
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
