package cmd

import (
	"context"

	"github.com/rerost/es-cli/cmd/copy"
	"github.com/rerost/es-cli/cmd/count"
	"github.com/rerost/es-cli/cmd/create"
	"github.com/rerost/es-cli/cmd/delete"
	"github.com/rerost/es-cli/cmd/dump"
	"github.com/rerost/es-cli/cmd/list"
	"github.com/rerost/es-cli/cmd/restore"
	"github.com/rerost/es-cli/domain"
	"github.com/spf13/cobra"
)

func NewCmdRoot(ctx context.Context, ind domain.Index) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "es-cli",
		Short: "Elasticsearch control tool",
	}

	cmd.AddCommand(
		list.NewListCommand(ctx, ind),
		copy.NewCopyCommand(ctx, ind),
		count.NewCountCommand(ctx, ind),
		create.NewCreateCommand(ctx, ind),
		delete.NewDeleteCommand(ctx, ind),
		dump.NewDumpCommand(ctx, ind),
		restore.NewRestoreCommand(ctx, ind),
	)

	return cmd
}
