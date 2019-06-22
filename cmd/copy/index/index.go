package copy

import (
	"context"

	"github.com/rerost/es-cli/domain"
	"github.com/spf13/cobra"
	"github.com/srvc/fail"
)

func NewIndexCmd(ctx context.Context, ind domain.Index) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "index",
		Short: "list up index",
		Args:  cobra.ExactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			err := ind.Copy(ctx, args[0], args[1])
			if err != nil {
				return fail.Wrap(err)
			}
			return nil
		},
	}

	return cmd
}
