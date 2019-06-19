package count

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
		Short: "count index",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			cnt, err := ind.Count(ctx, args[0])
			if err != nil {
				return fail.Wrap(err)
			}
			fmt.Fprintln(os.Stdout, cnt)
			return nil
		},
	}

	return cmd
}
