package logger

import (
	"github.com/rerost/es-cli/config"
	"github.com/srvc/fail"
	"go.uber.org/zap"
)

func NewLogger(cfg config.Config) (*zap.Logger, error) {
	zcfg := zap.NewProductionConfig()
	if cfg.Debug {
		zcfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	}
	if cfg.Verbose {
		zcfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	}
	l, err := zcfg.Build()
	return l, fail.Wrap(err)
}
