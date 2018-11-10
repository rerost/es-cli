package es

import "context"

type Query struct{}

type ExperimentClient interface {
	// SQL
	TranslateSQL(ctx context.Context, sql string) (Query, error)
	// ExecSQL(ctx context.Context, sql string) (SQLResult, error)

	// Backup & Restore
	DumpIndex(ctx context.Context, indexName string, filePath string) error
	RestoreIndex(ctx context.Context, indexName string, filePath string) error
}
