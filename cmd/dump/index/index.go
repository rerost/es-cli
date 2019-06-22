package dump

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
		Short: "dump index",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(_ *cobra.Command, args []string) error {
			var fp io.Writer
			switch len(args) {
			case 1:
				fp = os.Stdout
			case 2:
				// Read file from filename
				fileName := args[1]
				var err error
				fp, err = os.Open(fileName)
				if err != nil {
					return fail.Wrap(err)
				}
			}
			err := ind.Dump(ctx, args[0], fp)
			if err != nil {
				return fail.Wrap(err)
			}
			return nil
		},
	}

	return cmd
}
