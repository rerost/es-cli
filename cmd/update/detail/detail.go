package update

import (
	"context"
	"io"
	"os"

	"github.com/rerost/es-cli/domain"
	"github.com/spf13/cobra"
	"github.com/srvc/fail"
)

func NewDetailCmd(ctx context.Context, dtl domain.Detail) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "detail",
		Short: "update detail",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(_ *cobra.Command, args []string) error {
			var detailFp io.Reader

			switch len(args) {
			case 1:
				detailFp = os.Stdin
			case 2:
				var err error
				detailFp, err = os.Open(args[1])
				if err != nil {
					return fail.Wrap(err)
				}
			}

			err := dtl.Update(ctx, args[0], detailFp)
			if err != nil {
				return fail.Wrap(err)
			}

			return nil
		},
	}

	return cmd
}
