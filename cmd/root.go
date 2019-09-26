package cmd

import (
	"context"
	"os"

	"github.com/rerost/es-cli/cmd/add"
	"github.com/rerost/es-cli/cmd/copy"
	"github.com/rerost/es-cli/cmd/count"
	"github.com/rerost/es-cli/cmd/create"
	"github.com/rerost/es-cli/cmd/delete"
	"github.com/rerost/es-cli/cmd/dump"
	"github.com/rerost/es-cli/cmd/get"
	"github.com/rerost/es-cli/cmd/list"
	"github.com/rerost/es-cli/cmd/remove"
	"github.com/rerost/es-cli/cmd/restore"
	"github.com/rerost/es-cli/cmd/update"
	"github.com/rerost/es-cli/domain"
	"github.com/spf13/cobra"
)

func NewCmdRoot(
	ctx context.Context,
	ind domain.Index,
	dtl domain.Detail,
	alis domain.Alias,
) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "es-cli",
		Short: "Elasticsearch control tool",
	}

	cmd.AddCommand(
		add.NewAddCommand(ctx, ind, alis),
		list.NewListCommand(ctx, ind, alis),
		copy.NewCopyCommand(ctx, ind),
		count.NewCountCommand(ctx, ind),
		create.NewCreateCommand(ctx, ind),
		delete.NewDeleteCommand(ctx, ind),
		dump.NewDumpCommand(ctx, ind),
		restore.NewRestoreCommand(ctx, ind),
		get.NewGetCommand(ctx, dtl),
		update.NewUpdateCommand(ctx, dtl),
		remove.NewRemoveCommand(ctx, alis),
		NewBashCmd(),
		NewZshCmd(),
	)

	return cmd
}
func NewBashCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "bash",
		Short: "Generates bash completion scripts",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.GenBashCompletion(os.Stdout)
		},
	}
}

func NewZshCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "zsh",
		Short: "Generates zsh completion scripts",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.GenZshCompletion(os.Stdout)
		},
	}
}
