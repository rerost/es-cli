package cmd

import (
	"context"

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

	return fail.Wrap(cmd.Execute())
}

func NewConfig() (config.Config, error) {
	v := viper.New()

	pflag.StringP("host", "", "localhost", "ES hostname")
	pflag.StringP("type", "t", "_doc", "ES type")
	pflag.StringP("user", "u", "localhost", "ES basic auth user")
	pflag.StringP("pass", "p", "localhost", "ES basic auth password")
	pflag.BoolP("insecure", "k", false, "Same as curl insecure")
	pflag.StringP("namespace", "n", "localhost", "Specify config in es-cli")

	pflag.BoolP("verbose", "v", false, "")
	pflag.BoolP("debug", "d", false, "")

	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)

	var cfg config.Config
	err := v.Unmarshal(&cfg)
	return cfg, err
}
