package domain

import (
	"context"
	"io"
	"io/ioutil"
	"time"

	"github.com/rerost/es-cli/infra/es"
	"github.com/srvc/fail"
)

type Detail interface {
	Get(ctx context.Context, index string) (string, error)
	Update(ctx context.Context, aliasName string, detail io.Reader) error
}

func NewDetail(esBaseClient es.BaseClient, indexDomain Index) Detail {
	return detailImpl{
		esBaseClient: esBaseClient,
		indexDomain:  indexDomain,
	}
}

type detailImpl struct {
	esBaseClient es.BaseClient
	indexDomain  Index
}

func (d detailImpl) Get(ctx context.Context, index string) (string, error) {
	detail, err := d.esBaseClient.DetailIndex(ctx, index)
	return detail.String(), fail.Wrap(err)
}

func (d detailImpl) Update(ctx context.Context, aliasName string, fp io.Reader) error {
	// Thinking only alias case
	// Rethink when index
	// TODO think index case

	var detailJSON string

	body, err := ioutil.ReadAll(fp)
	detailJSON = string(body)

	indices, err := d.esBaseClient.ListAlias(ctx, aliasName)
	if err != nil {
		return fail.Wrap(err)
	}

	if len(indices) != 1 {
		return fail.New("Support only 1-alias 1-index case")
	}

	oldIndexName := indices[0].Name

	newIndexName := aliasName + time.Now().Format("_20060102_150405")
	err = d.esBaseClient.CreateIndex(ctx, newIndexName, detailJSON)
	if err != nil {
		return fail.Wrap(err)
	}

	err = d.indexDomain.Copy(ctx, oldIndexName, newIndexName)
	if err != nil {
		return fail.Wrap(err)
	}
	err = d.esBaseClient.SwapAlias(ctx, aliasName, oldIndexName, newIndexName)
	if err != nil {
		return fail.Wrap(err)
	}
	err = d.esBaseClient.DeleteIndex(ctx, oldIndexName)
	if err != nil {
		return fail.Wrap(err)
	}
	return nil
}
