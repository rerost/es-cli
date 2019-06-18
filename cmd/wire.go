//+build wireinject

package cmd

import (
	"context"

	"github.com/google/wire"
	"github.com/rerost/es-cli/config"
	"github.com/rerost/es-cli/domain"
	"github.com/rerost/es-cli/infra/es"
	"github.com/rerost/es-cli/infra/http"
	"github.com/rerost/es-cli/infra/logger"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func InitializeCmd(ctx context.Context, cfg config.Config) (*cobra.Command, error) {
	wire.Build(NewCmdRoot, es.NewBaseClient, http.NewClient, domain.NewIndex)
	return &cobra.Command{}, nil
}

func InitializeLogger(cfg config.Config) (*zap.Logger, error) {
	wire.Build(logger.NewLogger)
	return &zap.Logger{}, nil
}
