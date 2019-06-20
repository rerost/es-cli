package update

import (
	"context"

	update "github.com/rerost/es-cli/cmd/update/detail"
	"github.com/rerost/es-cli/domain"
	"github.com/spf13/cobra"
)

func NewUpdateCommand(ctx context.Context, dtl domain.Detail) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update",
		Short: "Update up elasitcsearch resources",
		Args:  cobra.ExactArgs(1),
	}

	cmd.AddCommand(update.NewDetailCmd(ctx, dtl))
	return cmd
}
