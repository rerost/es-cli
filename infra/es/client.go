package es

import "context"

type Index struct {
	Name string
}
type Mapping struct{}
type Opt struct{}
type Alias struct{}
type Task struct{}
type Version struct{}

// Client is http wrapper
type BaseClient interface {
	// Index
	ListIndex(ctx context.Context, indexName string) ([]Index, error)
	CreateIndex(ctx context.Context, indexName string, mappingJSON string) error
	CopyIndex(ctx context.Context, srcIndexName string, dstIndexName string) error
	DeleteIndex(ctx context.Context, indexName string) error

	// Mapping
	GetMapping(ctx context.Context, indexOrAliasName string) (Mapping, error)
	// UpdateMapping(ctx context.Context, aliasName string, mappingJSON string) error

	// Alias
	CreateAlias(ctx context.Context, indexName string, aliasName string) error
	DropAlias(ctx context.Context, aliasName string, opts []Opt) error
	AddAlias(ctx context.Context, aliasName string, indexNames ...string) error
	RemoveAlias(ctx context.Context, aliasName string, indexNames ...string) error
	GetAlias(ctx context.Context, aliasName string) Alias

	// Task
	ListTask(ctx context.Context) ([]Task, error)
	GetTask(ctx context.Context, taskID string) (Task, error)

	Version(ctx context.Context) (Version, error)
	Ping(ctx context.Context) (bool, error)
}

func NewBaseClient(ctx context.Context) (BaseClient, error) {
}
