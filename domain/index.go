package domain

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/rerost/es-cli/infra/es"
	"github.com/srvc/fail"
	"go.uber.org/zap"
)

const (
	BATCH_SIZE  = 1000
	initBufSize = 65536
	maxBufSize  = 65536000
)

type Index interface {
	List(ctx context.Context) (es.Indices, error)
	Create(ctx context.Context, indexName string, mapping io.Reader) error
	Delete(ctx context.Context, indexName string) error
	Copy(ctx context.Context, srcIndex, destIndex string) error
	Count(ctx context.Context, indexName string) (int64, error)
	Dump(ctx context.Context, indexName string, fp io.Writer) error
	Restore(ctx context.Context, fp io.Reader) error
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
	{
		indices, err := i.esBaseClient.ListIndex(ctx)
		if err != nil {
			return fail.Wrap(err)
		}
		var srcExists bool
		var destExists bool

		zap.L().Debug(
			"Already exists index",
			zap.String("indicies", indices.String()),
		)
		// TODO Add test
		for _, index := range indices {
			zap.L().Debug(
				"Checking",
				zap.String("source index", srcIndex),
				zap.String("destination index", destIndex),
				zap.String("checking index", index.String()),
			)
			if index.String() == srcIndex {
				srcExists = true
			}
			if index.String() == destIndex {
				destExists = true
			}
		}

		if !srcExists {
			return fail.Wrap(fail.New("Source index is not found"), fail.WithParam("index", srcIndex))
		}
		if !destExists {
			return fail.Wrap(fail.New("Destination index is not found"), fail.WithParam("index", destIndex))
		}
	}
	task, err := i.esBaseClient.CopyIndex(ctx, srcIndex, destIndex)

	if err != nil {
		return fail.Wrap(err)
	}

	fmt.Fprintf(os.Stdout, "TaskID is %s\n", task.ID)
	zap.L().Debug("Start task", zap.String("task_id: ", task.ID))

	for try := 1; ; try++ {
		// Back off
		wait := time.Second * time.Duration(try*try)
		time.Sleep(wait)
		zap.L().Debug("Waiting for complete copy", zap.Duration("waited(s)", wait))
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

func (i indexImpl) Count(ctx context.Context, indexName string) (int64, error) {
	c, err := i.esBaseClient.CountIndex(ctx, indexName)
	if err != nil {
		return 0, fail.Wrap(err)
	}

	return c.Num, nil
}

func (i indexImpl) Dump(ctx context.Context, indexName string, detailFile io.Writer) error {
	detail, err := i.esBaseClient.DetailIndex(ctx, indexName)
	if err != nil {
		return fail.Wrap(err)
	}

	_, err = detailFile.Write([]byte(detail.String()))
	if err != nil {
		return fail.Wrap(err)
	}

	dumpFile, err := os.Create(fmt.Sprintf("./%s_dump.ndjson", indexName))
	if err != nil {
		return fail.Wrap(err)
	}
	defer dumpFile.Close()

	lastID := ""
	for {
		query := fmt.Sprintf(`{"query": {"match_all": {}}, "size": %d, "sort": [{"_id": "desc"}]}`, BATCH_SIZE)
		if lastID != "" {
			zap.L().Info("Copying search after", zap.String("ID", lastID))
			query = fmt.Sprintf(`{"query": {"match_all": {}}, "size": %d, "sort": [{"_id": "desc"}], "search_after": ["%s"]}`, BATCH_SIZE, lastID)
		}
		searchResult, err := i.esBaseClient.SearchIndex(ctx, indexName, query)

		if err != nil {
			return fail.Wrap(err)
		}

		for _, hit := range searchResult.Hits.Hits {
			metaData := fmt.Sprintf(`{ "index" : { "_index": "%s", "_type": "%s", "_id": "%s" }}`, hit.Index, hit.Type, hit.ID)
			queryBytes, err := json.Marshal(hit.Source)
			if err != nil {
				return fail.Wrap(err)
			}
			_, err = dumpFile.Write([]byte(metaData + "\n" + string(queryBytes) + "\n"))
			if err != nil {
				return fail.Wrap(err)
			}
		}

		hitsSize := len(searchResult.Hits.Hits)
		if hitsSize == 0 {
			break
		}

		lastID = searchResult.Hits.Hits[hitsSize-1].ID
	}

	return nil
}

func (i indexImpl) Restore(ctx context.Context, fp io.Reader) error {
	scanner := bufio.NewScanner(fp)

	scanner.Split(bufio.ScanLines)
	{
		buf := make([]byte, initBufSize)
		scanner.Buffer(buf, maxBufSize)
	}

	// twice, because metadata + document pair
	buf := make([]string, BATCH_SIZE*2, BATCH_SIZE*2)
	iter := 0
	batchTime := 1
	for scanner.Scan() {
		buf[iter] = scanner.Text()

		if iter == len(buf)-1 {
			zap.L().Debug("Copied", zap.Int("size", len(buf)/2*batchTime))
			err := i.esBaseClient.BulkIndex(ctx, strings.Join(buf, "\n")+"\n")
			if err != nil {
				return fail.Wrap(err)
			}

			buf = make([]string, len(buf), len(buf))
			iter = 0
			batchTime++
		} else {
			iter++
		}
	}
	if err := scanner.Err(); err != nil {
		return fail.Wrap(err)
	}
	return nil
}
