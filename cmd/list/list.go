package list

import (
	"context"

	alist "github.com/rerost/es-cli/cmd/list/alias" // FIXME pkg name
	ilist "github.com/rerost/es-cli/cmd/list/index"
	"github.com/rerost/es-cli/domain"
	"github.com/spf13/cobra"
)

func NewListCommand(ctx context.Context, ind domain.Index, alis domain.Alias) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List up elasitcsearch resources",
		Args:  cobra.ExactArgs(1),
	}

	cmd.AddCommand(ilist.NewIndexCmd(ctx, ind)) // TODO Cmd -> Command
	cmd.AddCommand(alist.NewAliasCommand(ctx, alis))
	return cmd
}
