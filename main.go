package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/rerost/es-cli/config"
	"github.com/rerost/es-cli/executer"
	"github.com/rerost/es-cli/infra/es"
	"github.com/srvc/fail"
	"github.com/urfave/cli"
	"gopkg.in/guregu/null.v3"
)

func main() {
	app := cli.NewApp()

	app.Name = "es-cli"
	app.Usage = "TODO"

	app.Action = func(cliContext *cli.Context) error {
		ctx := context.Background()

		cfg := config.DefaultConfig()

		// Check namespace
		namespace := cliContext.String("namespace")

		// Local Config file
		if f, err := ioutil.ReadFile(".escli.json"); err == nil {
			if namespace == "" {
				localCfg, err := config.LoadConfig(f)
				if err != nil {
					return fail.Wrap(err)
				}
				cfg = config.Overwrite(cfg, localCfg)
			} else {
				localCfg, err := config.LoadConfigWithNamespace(f, namespace)
				if err != nil {
					return fail.Wrap(err)
				}
				cfg = config.Overwrite(cfg, localCfg)
			}
		}

		// Params Config
		paramsCfg := config.Config{
			Host:     cliContext.String("host"),
			Type:     cliContext.String("type"),
			User:     cliContext.String("user"),
			Pass:     cliContext.String("pass"),
			Insecure: null.BoolFrom(cliContext.Bool("insecure")),
		}
		cfg = config.Overwrite(cfg, paramsCfg)

		var httpClient *http.Client
		if cfg.Insecure.Valid && cfg.Insecure.Bool {
			tr := &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			}
			httpClient = &http.Client{Transport: tr}
		} else {
			httpClient = new(http.Client)
		}

		esBaseClient, err := es.NewBaseClient(cfg, httpClient)
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
		cli.StringFlag{
			Name:  "namespace, n",
			Usage: "Specify namespace in es-cli",
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
