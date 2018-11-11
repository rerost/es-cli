package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/rerost/es-cli/executer"
	"github.com/rerost/es-cli/infra/es"
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

		// Default Value
		ctx = context.WithValue(ctx, setting.SettingKey("host"), "http://localhost")
		ctx = context.WithValue(ctx, setting.SettingKey("port"), "9200")
		ctx = context.WithValue(ctx, setting.SettingKey("type"), "_doc")
		ctx = context.WithValue(ctx, setting.SettingKey("user"), "")
		ctx = context.WithValue(ctx, setting.SettingKey("pass"), "")

		// Home config file
		if homeDir := os.Getenv("HOME"); homeDir != "" {
			if f, err := ioutil.ReadFile(homeDir + "/.escli.json"); err == nil {
				configMap := map[string]string{}
				err = json.Unmarshal(f, &configMap)
				if err != nil {
					return fail.Wrap(err)
				}

				for k, v := range configMap {
					ctx = context.WithValue(ctx, setting.SettingKey(k), v)
				}
			}
		}

		// Config file
		if f, err := ioutil.ReadFile(".escli.json"); err == nil {
			configMap := map[string]string{}
			err = json.Unmarshal(f, &configMap)
			if err != nil {
				return fail.Wrap(err)
			}

			for k, v := range configMap {
				ctx = context.WithValue(ctx, setting.SettingKey(k), v)
			}
		}

		// Params
		_host := cliContext.String("host")
		if _host != "" {
			ctx = context.WithValue(ctx, setting.SettingKey("host"), _host)
		}
		_port := cliContext.String("port")
		if _port != "" {
			ctx = context.WithValue(ctx, setting.SettingKey("port"), _port)
		}
		_type := cliContext.String("type")
		if _type != "" {
			ctx = context.WithValue(ctx, setting.SettingKey("type"), _type)
		}
		_user := cliContext.String("user")
		if _user != "" {
			ctx = context.WithValue(ctx, setting.SettingKey("user"), _user)
		}
		_pass := cliContext.String("pass")
		if _pass != "" {
			ctx = context.WithValue(ctx, setting.SettingKey("pass"), cliContext.String("pass"))
		}

		esBaseClient, err := es.NewBaseClient(ctx, new(http.Client))
		if err != nil {
			return err
		}

		head := cliContext.Args().First()
		args := cliContext.Args().Tail()

		if head == "" {
			cli.ShowAppHelp(cliContext)
			return fail.Wrap(fail.New("You need <operation>"), fail.WithCode("Invalid arguments"))
		}
		operation := head

		head = cli.Args(args).First()
		args = cli.Args(args).Tail()
		if head == "" {
			cli.ShowAppHelp(cliContext)
			return fail.Wrap(fail.New("You need <target>"), fail.WithCode("Invalid arguments"))
		}
		target := head

		e := executer.NewExecuter(esBaseClient)
		result, err := e.Run(ctx, operation, target, args)

		if fail.Unwrap(err).Code == "Invalid arguments" {
			cli.ShowAppHelp(cliContext)
		}
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
			Name:  "type, t",
			Usage: "Elasticsearch documents type",
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
