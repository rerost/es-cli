package restore

import (
	"context"
	"io"
	"os"

	"github.com/rerost/es-cli/domain"
	"github.com/spf13/cobra"
	"github.com/srvc/fail"
)

func NewIndexCmd(ctx context.Context, ind domain.Index) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "index",
		Short: "restore index",
		Args:  cobra.RangeArgs(0, 1),
		RunE: func(_ *cobra.Command, args []string) error {
			var fp io.Reader
			switch len(args) {
			case 1:
				fp = os.Stdin
			case 2:
				// Read file from filename
				fileName := args[0]
				var err error
				fp, err = os.Open(fileName)
				if err != nil {
					return fail.Wrap(err)
				}
			}
			err := ind.Restore(ctx, fp)
			if err != nil {
				return fail.Wrap(err)
			}
			return nil
		},
	}

	return cmd
}
