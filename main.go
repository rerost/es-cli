package main

import (
	"context"
	"crypto/tls"
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
		ctx = setting.ContextWithOptions(
			ctx,
			cliContext.String("host"),
			cliContext.String("port"),
			cliContext.String("type"),
			cliContext.String("user"),
			cliContext.String("pass"),
		)

		ctx = context.WithValue(ctx, setting.SettingKey("insecure"), cliContext.Bool("insecure"))
		var httpClient *http.Client
		if cliContext.Bool("insecure") {
			tr := &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			}
			httpClient = &http.Client{Transport: tr}
		} else {
			httpClient = new(http.Client)
		}

		esBaseClient, err := es.NewBaseClient(ctx, httpClient)
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

		e := executer.NewExecuter(esBaseClient, httpClient)
		result, err := e.Run(ctx, operation, target, args)

		if err != nil && fail.Unwrap(err).Code == "Invalid arguments" {
			cli.ShowAppHelp(cliContext)
		}
		fmt.Fprintf(os.Stdout, result.String())
		return fail.Wrap(err)
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
			Name:  "pass, P",
			Usage: "ES basic auth password",
		},
		cli.BoolFlag{
			Name:  "insecure, k",
			Usage: "Same as curl insecure",
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
