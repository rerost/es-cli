package create

import (
	"context"

	create "github.com/rerost/es-cli/cmd/create/index"
	"github.com/rerost/es-cli/domain"
	"github.com/spf13/cobra"
)

func NewCreateCommand(ctx context.Context, ind domain.Index) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create elasticsearch resources",
		Args:  cobra.ExactArgs(2),
	}

	cmd.AddCommand(create.NewIndexCmd(ctx, ind))
	return cmd
}
