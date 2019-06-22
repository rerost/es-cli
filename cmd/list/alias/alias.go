package alias

import (
	"context"
	"fmt"
	"os"

	"github.com/rerost/es-cli/domain"
	"github.com/spf13/cobra"
	"github.com/srvc/fail"
)

func NewAliasCommand(ctx context.Context, alis domain.Alias) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "alias",
		Short: "list up index in alias",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			indices, err := alis.List(ctx, args[0])
			if err != nil {
				return fail.Wrap(err)
			}
			fmt.Fprintln(os.Stdout, indices.String())
			return nil
		},
	}

	return cmd
}
