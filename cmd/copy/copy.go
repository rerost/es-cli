package copy

import (
	"context"

	copy "github.com/rerost/es-cli/cmd/copy/index"
	"github.com/rerost/es-cli/domain"
	"github.com/spf13/cobra"
)

func NewCopyCommand(ctx context.Context, ind domain.Index) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "copy",
		Short: "Copy elasticsearch resources",
		Args:  cobra.ExactArgs(2),
	}

	cmd.AddCommand(copy.NewIndexCmd(ctx, ind))
	return cmd
}
