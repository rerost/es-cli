package executer

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/rerost/es-cli/infra/es"
	"github.com/rerost/es-cli/setting"
	"github.com/srvc/fail"
)

type Result interface {
	String() string
}
type Args []string

type Executer interface {
	Run(ctx context.Context, operation string, target string, args Args) (Result, error)
}

type Empty struct{}
type Command struct {
	ArgLen  int
	ArgType ArgTypes
}

type ArgTypes int

const (
	EXACT ArgTypes = iota
	MORE
	LESS
	STDIN
)

const (
	BATCH_SIZE = 1000
)

func (c Command) Validate(args Args) error {
	if c.ArgType == EXACT {
		if len(args) != c.ArgLen {
			return fail.Wrap(fail.New(fmt.Sprintf("Invalid arguments expected: %d, %v", c.ArgLen, args)), fail.WithCode("Invalid arguments"))
		}
	}
	if c.ArgType == MORE {
		if !(len(args) >= c.ArgLen) {
			return fail.Wrap(fail.New(fmt.Sprintf("Invalid arguments more than: %d, %v", c.ArgLen, args)), fail.WithCode("Invalid arguments"))
		}
	}
	if c.ArgType == STDIN {
		// Stdin
		if !(len(args) == c.ArgLen-1 || len(args) == c.ArgLen) {
			return fail.Wrap(fail.New(fmt.Sprintf("Invalid arguments less or much: %d, %v", c.ArgLen, args)), fail.WithCode("Invalid arguments"))
		}
	}
	return nil
}

func (e Empty) String() string {
	return ""
}

type executerImp struct {
	esBaseClient es.BaseClient
	httpClient   *http.Client
}

func NewExecuter(esBaseClient es.BaseClient, httpClient *http.Client) Executer {
	return &executerImp{esBaseClient: esBaseClient, httpClient: httpClient}
}

var CommandMap map[string]map[string]Command

func init() {
	CommandMap = map[string]map[string]Command{
		"index": {
			"list":    Command{ArgLen: 0, ArgType: EXACT},
			"create":  Command{ArgLen: 2, ArgType: STDIN},
			"delete":  Command{ArgLen: 1, ArgType: EXACT},
			"copy":    Command{ArgLen: 2, ArgType: EXACT},
			"count":   Command{ArgLen: 1, ArgType: EXACT},
			"dump":    Command{ArgLen: 1, ArgType: EXACT},
			"restore": Command{ArgLen: 1, ArgType: STDIN},
		},
		"detail": {
			"update": Command{ArgLen: 2, ArgType: STDIN},
			"get":    Command{ArgLen: 1, ArgType: EXACT},
		},
		"alias": {
			"add":    Command{ArgLen: 2, ArgType: MORE},
			"remove": Command{ArgLen: 2, ArgType: MORE},
			"list":   Command{ArgLen: 1, ArgType: EXACT},
		},
		"task": {
			"list": Command{ArgLen: 0, ArgType: EXACT},
			"get":  Command{ArgLen: 1, ArgType: EXACT},
		},
		"version": {
			"get": Command{ArgLen: 0, ArgType: EXACT},
		},
		"ping": {
			"check": Command{ArgLen: 0, ArgType: EXACT},
		},
		"remote": {
			"copy": Command{ArgLen: 6, ArgType: STDIN},
		},
	}
}

func (e *executerImp) Run(ctx context.Context, operation string, target string, args Args) (Result, error) {
	if c, ok := CommandMap[target][operation]; !ok {
		return Empty{}, fail.Wrap(fail.New(fmt.Sprintf("Invalid target and operation %v %v", operation, target)), fail.WithCode("Invalid arguments"))
	} else if err := c.Validate(args); err != nil {
		return Empty{}, err
	}

	pong, err := e.esBaseClient.Ping(ctx)
	if err != nil {
		return Empty{}, err
	}
	if !pong.OK {
		return Empty{}, fail.New("Connection Failed")
	}

	if target == "index" {
		switch operation {
		case "list":
			return e.esBaseClient.ListIndex(ctx)
		case "create":
			var fp *os.File
			if len(args) == CommandMap[target][operation].ArgLen {
				fp, err = os.Open(args[1])
				if err != nil {
					return Empty{}, fail.Wrap(err)
				}
			} else if len(args) == CommandMap[target][operation].ArgLen-1 {
				fp = os.Stdin
			}
			body, err := ioutil.ReadAll(fp)
			if err != nil {
				return Empty{}, fail.Wrap(err)
			}

			return Empty{}, e.esBaseClient.CreateIndex(ctx, args[0], string(body))
		case "delete":
			return Empty{}, e.esBaseClient.DeleteIndex(ctx, args[0])
		case "copy":
			task, err := e.esBaseClient.CopyIndex(ctx, args[0], args[1])

			if err != nil {
				return Empty{}, fail.Wrap(err)
			}

			fmt.Fprintf(os.Stdout, "TaskID is %s\n", task.ID)

			for i := 1; ; i++ {
				// Back off
				time.Sleep(time.Second * time.Duration(i*i))
				fmt.Fprintf(os.Stdout, "Waiting for complete copy...\n")
				task, err := e.esBaseClient.GetTask(ctx, task.ID)

				if err != nil {
					return Empty{}, fail.Wrap(err)
				}

				if task.Complete == true {
					break
				}
			}
			srcIndexCount, err := e.esBaseClient.CountIndex(ctx, args[0])
			if err != nil {
				return Empty{}, fail.Wrap(err)
			}
			dstIndexCount, err := e.esBaseClient.CountIndex(ctx, args[0])
			if err != nil {
				return Empty{}, fail.Wrap(err)
			}
			if srcIndexCount.Num != dstIndexCount.Num {
				return Empty{}, fail.Wrap(fail.New(fmt.Sprintf("Copy is faild. Prease delete index %s Not match document count src: %d, dst: %d", args[1], srcIndexCount.Num, dstIndexCount.Num)), fail.WithCode("Invalid arguments"))
			}
			fmt.Fprintf(os.Stdout, "Done copy")

			return Empty{}, nil
		case "count":
			return e.esBaseClient.CountIndex(ctx, args[0])
		case "dump":
			indexName := args[0]

			detail, err := e.esBaseClient.DetailIndex(ctx, indexName)
			if err != nil {
				return Empty{}, fail.Wrap(err)
			}
			detailFile, err := os.Create(fmt.Sprintf("./%s_detail.json", indexName))
			if err != nil {
				return Empty{}, fail.Wrap(err)
			}
			_, err = detailFile.Write([]byte(detail.String()))
			if err != nil {
				return Empty{}, fail.Wrap(err)
			}
			detailFile.Close()

			dumpFile, err := os.Create(fmt.Sprintf("./%s_dump.ndjson", indexName))
			if err != nil {
				return Empty{}, fail.Wrap(err)
			}
			defer dumpFile.Close()

			lastID := ""
			for {
				query := fmt.Sprintf(`{"query": {"match_all": {}}, "size": %d, "sort": [{"_id": "desc"}]}`, BATCH_SIZE)
				if lastID != "" {
					fmt.Printf("Copying search after %s\n", lastID)
					query = fmt.Sprintf(`{"query": {"match_all": {}}, "size": %d, "sort": [{"_id": "desc"}], "search_after": ["%s"]}`, BATCH_SIZE, lastID)
				}
				searchResult, err := e.esBaseClient.SearchIndex(ctx, indexName, query)

				if err != nil {
					return Empty{}, fail.Wrap(err)
				}

				for _, hit := range searchResult.Hits.Hits {
					metaData := fmt.Sprintf(`{ "index" : { "_index": "%s", "_type": "%s", "_id": "%s" }}`, hit.Index, hit.Type, hit.ID)
					queryBytes, err := json.Marshal(hit.Source)
					if err != nil {
						return Empty{}, fail.Wrap(err)
					}
					_, err = dumpFile.Write([]byte(metaData + "\n" + string(queryBytes) + "\n"))
					if err != nil {
						return Empty{}, fail.Wrap(err)
					}
				}

				hitsSize := len(searchResult.Hits.Hits)
				if hitsSize == 0 {
					break
				}

				lastID = searchResult.Hits.Hits[hitsSize-1].ID
			}

			return Empty{}, nil
		case "restore":
			fmt.Println("**Waring** Prease create index before restore")
			var fp *os.File
			if len(args) == 0 {
				fp = os.Stdin
			} else {
				fp, err = os.Open(args[0])
				if err != nil {
					return Empty{}, fail.Wrap(err)
				}
			}
			defer fp.Close()

			scanner := bufio.NewScanner(fp)
			scanner.Split(bufio.ScanLines)

			// twice, because metadata + document pair
			buf := make([]string, BATCH_SIZE*2, BATCH_SIZE*2)
			i := 0
			batchTime := 1
			for scanner.Scan() {
				buf[i] = scanner.Text()

				if i == len(buf)-1 {
					fmt.Printf("Copied %d\n", len(buf)/2*batchTime)
					err := e.esBaseClient.BulkIndex(ctx, strings.Join(buf, "\n")+"\n")
					if err != nil {
						return Empty{}, fail.Wrap(err)
					}

					buf = make([]string, len(buf), len(buf))
					i = 0
					batchTime++
				} else {
					i++
				}
			}
			return Empty{}, nil
		default:
			return Empty{}, fail.Wrap(fail.New(fmt.Sprintf("Invalid operation: %v", operation)), fail.WithCode("Invalid arguments"))
		}
	}

	if target == "detail" {
		switch operation {
		case "get":
			return e.esBaseClient.DetailIndex(ctx, args[0])
		case "update":
			// Thinking only alias case
			// Rethink when index
			// TODO think index case

			var aliasName, detailJSON string
			aliasName = args[0]
			var fp *os.File
			if len(args) == CommandMap[target][operation].ArgLen {
				fp, err = os.Open(args[1])
				if err != nil {
					return Empty{}, fail.Wrap(err)
				}
			} else if len(args) == CommandMap[target][operation].ArgLen-1 {
				fp = os.Stdin
			}

			body, err := ioutil.ReadAll(fp)
			fp.Close()
			if err != nil {
				return Empty{}, fail.Wrap(err)
			}
			detailJSON = string(body)

			indices, err := e.esBaseClient.ListAlias(ctx, aliasName)
			if err != nil {
				return Empty{}, fail.Wrap(err)
			}

			if len(indices) != 1 {
				return Empty{}, fail.New("Support only 1-alias 1-index case")
			}

			oldIndexName := indices[0].Name

			newIndexName := aliasName + time.Now().Format("_20060102_150405")
			err = e.esBaseClient.CreateIndex(ctx, newIndexName, detailJSON)
			if err != nil {
				return Empty{}, fail.Wrap(err)
			}
			_, err = e.Run(ctx, "copy", "index", []string{oldIndexName, newIndexName})
			if err != nil {
				return Empty{}, fail.Wrap(err)
			}
			err = e.esBaseClient.AddAlias(ctx, aliasName, newIndexName)
			if err != nil {
				return Empty{}, fail.Wrap(err)
			}
			err = e.esBaseClient.RemoveAlias(ctx, aliasName, oldIndexName)
			if err != nil {
				return Empty{}, fail.Wrap(err)
			}
			err = e.esBaseClient.DeleteIndex(ctx, oldIndexName)
			if err != nil {
				return Empty{}, fail.Wrap(err)
			}
			return Empty{}, nil
		default:
			return Empty{}, fail.Wrap(fail.New(fmt.Sprintf("Invalid operation: %v", operation)), fail.WithCode("Invalid arguments"))
		}
	}

	if target == "alias" {
		switch operation {
		case "add":
			return Empty{}, e.esBaseClient.AddAlias(ctx, args[0], args[1:]...)
		case "remove":
			return Empty{}, e.esBaseClient.RemoveAlias(ctx, args[0], args[1:]...)
		case "list":
			return e.esBaseClient.ListAlias(ctx, args[0])
		default:
			return Empty{}, fail.Wrap(fail.New(fmt.Sprintf("Invalid operation: %v", operation)), fail.WithCode("Invalid arguments"))
		}
	}

	if target == "task" {
		switch operation {
		case "list":
			return e.esBaseClient.ListTask(ctx)
		case "get":
			return e.esBaseClient.GetTask(ctx, args[0])
		default:
			return Empty{}, fail.Wrap(fail.New(fmt.Sprintf("Invalid operation: %v", operation)), fail.WithCode("Invalid arguments"))
		}
	}

	if target == "version" {
		switch operation {
		case "get":
			return e.esBaseClient.Version(ctx)
		default:
			return Empty{}, fail.Wrap(fail.New(fmt.Sprintf("Invalid operation: %v", operation)), fail.WithCode("Invalid arguments"))
		}
	}

	if target == "ping" {
		switch operation {
		case "check":
			return e.esBaseClient.Ping(ctx)
		default:
			return Empty{}, fail.Wrap(fail.New(fmt.Sprintf("Invalid operation: %v", operation)), fail.WithCode("Invalid arguments"))
		}
	}

	if target == "remote" {
		switch operation {
		case "copy":
			host := args[0]
			port := args[1]
			indexName := args[2]
			user := args[3]
			pass := args[4]
			docType := "_doc"
			if len(args) == 6 {
				docType = args[5]
			}

			// For copy context
			cctx := context.WithValue(ctx, setting.SettingKey(""), nil)
			cctx = setting.ContextWithOptions(cctx, host, port, docType, user, pass)

			remoteClient, err := es.NewBaseClient(cctx, e.httpClient)
			if err != nil {
				return Empty{}, fail.Wrap(err)
			}

			detail, err := remoteClient.DetailIndex(cctx, indexName)
			if err != nil {
				return Empty{}, fail.Wrap(err)
			}

			err = e.esBaseClient.CreateIndex(cctx, indexName, detail.String())
			if err != nil {
				return Empty{}, fail.Wrap(err)
			}

			lastID := ""
			for {
				query := fmt.Sprintf(`{"query": {"match_all": {}}, "size": %d, "sort": [{"_id": "desc"}]}`, BATCH_SIZE)
				if lastID != "" {
					fmt.Printf("Copying search after %s\n", lastID)
					query = fmt.Sprintf(`{"query": {"match_all": {}}, "size": %d, "sort": [{"_id": "desc"}], "search_after": ["%s"]}`, BATCH_SIZE, lastID)
				}
				searchResult, err := remoteClient.SearchIndex(cctx, indexName, query)

				if err != nil {
					return Empty{}, fail.Wrap(err)
				}

				bulkQuery := ""
				for _, hit := range searchResult.Hits.Hits {
					metaData := fmt.Sprintf(`{ "index" : { "_index": "%s", "_type": "%s", "_id": "%s" }}`, hit.Index, hit.Type, hit.ID)
					queryBytes, err := json.Marshal(hit.Source)
					if err != nil {
						return Empty{}, fail.Wrap(err)
					}
					bulkQuery = bulkQuery + metaData + "\n" + string(queryBytes) + "\n"
				}

				err = e.esBaseClient.BulkIndex(ctx, bulkQuery)
				if err != nil {
					return Empty{}, fail.Wrap(err)
				}

				fmt.Printf("Done copy search after %s\n", lastID)

				hitsSize := len(searchResult.Hits.Hits)
				if hitsSize == 0 {
					break
				}
				lastID = searchResult.Hits.Hits[hitsSize-1].ID
			}

			srcCnt, err := remoteClient.CountIndex(cctx, indexName)
			if err != nil {
				return Empty{}, fail.Wrap(err)
			}
			dstCnt, err := e.esBaseClient.CountIndex(ctx, indexName)
			if err != nil {
				return Empty{}, fail.Wrap(err)
			}

			if srcCnt.Num != dstCnt.Num {
				e.esBaseClient.DeleteIndex(ctx, indexName)
				return Empty{}, fail.New("Failed to copy index(Not correct count)")
			}

			// NOTE: When different version of ES, Its correct?
			srcDetail, err := remoteClient.DetailIndex(cctx, indexName)
			if err != nil {
				return Empty{}, fail.Wrap(err)
			}
			dstDetail, err := e.esBaseClient.DetailIndex(ctx, indexName)
			if err != nil {
				return Empty{}, fail.Wrap(err)
			}

			if srcDetail.String() != dstDetail.String() {
				e.esBaseClient.DeleteIndex(ctx, indexName)
				return Empty{}, fail.New("Failed to copy index(Not correct detail)")
			}

			return Empty{}, nil
		default:
			return Empty{}, fail.Wrap(fail.New(fmt.Sprintf("Invalid operation: %v", operation)), fail.WithCode("Invalid arguments"))
		}
	}

	return Empty{}, fail.Wrap(fail.New(fmt.Sprintf("Invalid target and operation %v %v", operation, target)), fail.WithCode("Invalid arguments"))
}
