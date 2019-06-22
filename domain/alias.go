package domain

import (
	"context"

	"github.com/rerost/es-cli/infra/es"
	"github.com/srvc/fail"
)

type Alias interface {
	Add(ctx context.Context, aliasName string, indexNames ...string) error
	Remove(ctx context.Context, aliasName string, indexNames ...string) error
	// List all index in alias
	List(ctx context.Context, aliasName string) (es.Indices, error)
}

func NewAlias(esBaseClient es.BaseClient) Alias {
	return aliasImpl{
		esBaseClient: esBaseClient,
	}
}

type aliasImpl struct {
	esBaseClient es.BaseClient
}

func (a aliasImpl) Add(ctx context.Context, aliasName string, indexNames ...string) error {
	err := a.esBaseClient.AddAlias(ctx, aliasName)
	return fail.Wrap(err)
}

func (a aliasImpl) Remove(ctx context.Context, aliasName string, indexNames ...string) error {
	err := a.esBaseClient.RemoveAlias(ctx, aliasName)
	return fail.Wrap(err)
}

func (a aliasImpl) List(ctx context.Context, aliasName string) (es.Indices, error) {
	indices, err := a.esBaseClient.ListAlias(ctx, aliasName)
	if err != nil {
		return nil, fail.Wrap(err)
	}
	return indices, nil
}
