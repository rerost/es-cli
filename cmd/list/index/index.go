package list

import (
	"context"
	"fmt"
	"os"

	"github.com/rerost/es-cli/domain"
	"github.com/spf13/cobra"
	"github.com/srvc/fail"
)

func NewIndexCmd(ctx context.Context, ind domain.Index) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "index",
		Short: "list up index",
		Args:  cobra.ExactArgs(0),
		RunE: func(_ *cobra.Command, args []string) error {
			indices, err := ind.List(ctx)
			if err != nil {
				return fail.Wrap(err)
			}
			fmt.Fprintln(os.Stdout, indices.String())
			return nil
		},
	}

	return cmd
}
