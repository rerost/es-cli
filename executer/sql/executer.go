package mapping

import "context"

type SQLResult struct{}
type SQL interface {
	ExecSQL(ctx context.Context, sql string) (SQLResult, error)
}
