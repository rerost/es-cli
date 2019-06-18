package cmd

import (
	"context"

	"github.com/rerost/es-cli/cmd/list"
	"github.com/rerost/es-cli/domain"
	"github.com/spf13/cobra"
)

func NewCmdRoot(ctx context.Context, ind domain.Index) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "es-cli",
		Short: "Elasticsearch control tool",
	}

	cmd.AddCommand(list.NewListCommand(ctx, ind))

	return cmd
}
