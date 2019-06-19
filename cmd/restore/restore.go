package restore

import (
	"context"

	restore "github.com/rerost/es-cli/cmd/restore/index"
	"github.com/rerost/es-cli/domain"
	"github.com/spf13/cobra"
)

func NewRestoreCommand(ctx context.Context, ind domain.Index) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "restore",
		Short: "Restore elasticsearch resources",
		Args:  cobra.ExactArgs(1),
	}

	cmd.AddCommand(restore.NewIndexCmd(ctx, ind))
	return cmd
}
