package get

import (
	"context"
	"fmt"
	"os"

	"github.com/rerost/es-cli/domain"
	"github.com/spf13/cobra"
	"github.com/srvc/fail"
)

func NewDetailCmd(ctx context.Context, dtl domain.Detail) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "detail",
		Short: "get detail",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			detail, err := dtl.Get(ctx, args[0])
			if err != nil {
				return fail.Wrap(err)
			}

			fmt.Fprintf(os.Stdout, detail)

			return nil
		},
	}

	return cmd
}
