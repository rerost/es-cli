package cmd

import (
	"context"
	"fmt"

	"github.com/rerost/es-cli/config"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/srvc/fail"
	"go.uber.org/zap"
)

func Run() error {
	ctx := context.TODO()

	cfg, err := NewConfig()
	if err != nil {
		return fail.Wrap(err)
	}

	l, err := InitializeLogger(cfg)
	if err != nil {
		return fail.Wrap(err)
	}
	defer l.Sync()

	zap.ReplaceGlobals(l)
	cmd, err := InitializeCmd(ctx, cfg)
	if err != nil {
		return fail.Wrap(err)
	}

	err = cmd.Execute()
	zap.L().Debug("error", zap.String("stack trace", fmt.Sprintf("%#v\n", err)))
	return fail.Wrap(err)
}

func NewConfig() (config.Config, error) {
	pflag.StringP("host", "", "http://localhost:9200", "ES hostname")
	pflag.StringP("type", "t", "_doc", "ES type")
	pflag.StringP("user", "u", "", "ES basic auth user")
	pflag.StringP("pass", "p", "", "ES basic auth password")
	pflag.BoolP("insecure", "k", false, "Same as curl insecure")
	pflag.StringP("namespace", "n", "localhost", "Specify config in es-cli") // For conf. Think alter position

	pflag.BoolP("verbose", "v", false, "")
	pflag.BoolP("debug", "d", false, "")

	viper.BindPFlags(pflag.CommandLine)

	var cfg config.Config
	pflag.Parse()
	err := viper.Unmarshal(&cfg)
	return cfg, fail.Wrap(err)
}
