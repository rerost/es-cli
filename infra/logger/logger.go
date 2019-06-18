package logger

import (
	"github.com/rerost/es-cli/config"
	"github.com/srvc/fail"
	"go.uber.org/zap"
)

func NewLogger(cfg config.Config) (*zap.Logger, error) {
	zcfg := zap.NewDevelopmentConfig()
	l, err := zcfg.Build()
	return l, fail.Wrap(err)
}
