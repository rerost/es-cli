package alias

import (
	"context"

	"github.com/rerost/es-cli/domain"
	"github.com/spf13/cobra"
	"github.com/srvc/fail"
)

func NewAliasCommand(ctx context.Context, alis domain.Alias) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "alias",
		Short: "unlink alias",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			err := alis.Remove(ctx, args[0], args[1:]...)
			return fail.Wrap(err)
		},
	}

	return cmd
}
