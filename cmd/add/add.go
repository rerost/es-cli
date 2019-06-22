package add

import (
	"context"

	"github.com/rerost/es-cli/cmd/add/alias"
	"github.com/rerost/es-cli/domain"
	"github.com/spf13/cobra"
)

func NewAddCommand(ctx context.Context, ind domain.Index, alis domain.Alias) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "add",
		Short: "Add elasitcsearch resources",
		Args:  cobra.ExactArgs(1),
	}

	cmd.AddCommand(alias.NewAliasCommand(ctx, alis)) // FIXME Cmd -> Command
	return cmd
}
