package domain

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/rerost/es-cli/infra/es"
	"github.com/srvc/fail"
	"go.uber.org/zap"
)

type Index interface {
	List(ctx context.Context) (es.Indices, error)
	Create(ctx context.Context, indexName string, mapping io.Reader) error
	Delete(ctx context.Context, indexName string) error
	Copy(ctx context.Context, srcIndex, destIndex string) error
}

func NewIndex(esBaseClient es.BaseClient) Index {
	return indexImpl{
		esBaseClient: esBaseClient,
	}
}

type indexImpl struct {
	esBaseClient es.BaseClient
}

func (i indexImpl) List(ctx context.Context) (es.Indices, error) {
	return i.esBaseClient.ListIndex(ctx)
}

func (i indexImpl) Create(ctx context.Context, indexName string, mapping io.Reader) error {
	b, err := ioutil.ReadAll(mapping)
	if err != nil {
		return fail.Wrap(err)
	}

	err = i.esBaseClient.CreateIndex(ctx, indexName, string(b))
	return fail.Wrap(err)
}

func (i indexImpl) Delete(ctx context.Context, indexName string) error {
	err := i.esBaseClient.DeleteIndex(ctx, indexName)
	return fail.Wrap(err)
}

func (i indexImpl) Copy(ctx context.Context, srcIndex, destIndex string) error {
	task, err := i.esBaseClient.CopyIndex(ctx, srcIndex, destIndex)

	if err != nil {
		return fail.Wrap(err)
	}

	fmt.Fprintf(os.Stdout, "TaskID is %s\n", task.ID)
	zap.L().Info("Start task", zap.String("task_id: ", task.ID))

	for try := 1; ; try++ {
		// Back off
		wait := time.Second * time.Duration(try*try)
		time.Sleep(wait)
		zap.L().Info("Waiting for complete copy", zap.Duration("waited(s)", wait))
		task, err := i.esBaseClient.GetTask(ctx, task.ID)

		if err != nil {
			return fail.Wrap(err)
		}

		if task.Complete == true {
			break
		}
	}
	srcIndexCount, err := i.esBaseClient.CountIndex(ctx, srcIndex)
	if err != nil {
		return fail.Wrap(err)
	}
	dstIndexCount, err := i.esBaseClient.CountIndex(ctx, destIndex)
	if err != nil {
		return fail.Wrap(err)
	}
	if srcIndexCount.Num != dstIndexCount.Num {
		return fail.Wrap(
			fail.New(fmt.Sprintf("Copy is faild. Prease delete index %s Not match document count src: %d, dst: %d", destIndex, srcIndexCount.Num, dstIndexCount.Num)),
			fail.WithCode("Invalid arguments"),
		)
	}
	zap.L().Info("Done")

	return nil
}
