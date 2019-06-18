package list

import (
	"context"

	list "github.com/rerost/es-cli/cmd/list/index"
	"github.com/rerost/es-cli/domain"
	"github.com/spf13/cobra"
)

func NewListCommand(ctx context.Context, ind domain.Index) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List up elasitcsearch resources",
		Args:  cobra.ExactArgs(1),
	}

	cmd.AddCommand(list.NewIndexCmd(ctx, ind))
	return cmd
}
