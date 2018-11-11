package executer

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/rerost/es-cli/infra/es"
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

func (e Empty) String() string {
	return ""
}

type executerImp struct {
	esBaseClient es.BaseClient
}

func NewExecuter(esBaseClient es.BaseClient) Executer {
	return &executerImp{esBaseClient: esBaseClient}
}

var CommandMap map[string]map[string]Command

func init() {
	CommandMap = map[string]map[string]Command{
		"index": {
			"list":   Command{ArgLen: 0, ArgType: EXACT},
			"create": Command{ArgLen: 2, ArgType: EXACT},
			"delete": Command{ArgLen: 1, ArgType: EXACT},
			"copy":   Command{ArgLen: 2, ArgType: EXACT},
			"count":  Command{ArgLen: 1, ArgType: EXACT},
		},
		"mapping": {
			"get":    Command{ArgLen: 1, ArgType: EXACT},
			"update": Command{ArgLen: 2, ArgType: EXACT},
		},
		"alias": {
			"create": Command{ArgLen: 2, ArgType: EXACT},
			"drop":   Command{ArgLen: 2, ArgType: EXACT},
			"add":    Command{ArgLen: 2, ArgType: MORE},
			"remove": Command{ArgLen: 2, ArgType: MORE},
		},
		"task": {
			"list": Command{ArgLen: 0, ArgType: EXACT},
			"get":  Command{ArgLen: 1, ArgType: EXACT},
		},
	}
}

func (e *executerImp) Run(ctx context.Context, operation string, target string, args Args) (Result, error) {
	if target == "index" {
		switch operation {
		case "list":
			return e.esBaseClient.ListIndex(ctx)
		case "create":
			if len(args) != 2 {
				return Empty{}, fail.Wrap(fail.New(fmt.Sprintf("Invalid arguments expected: %d, %v", 2, args)), fail.WithCode("Invalid arguments"))
			}
			return Empty{}, e.esBaseClient.CreateIndex(ctx, args[0], args[1])
		case "delete":
			if len(args) != 1 {
				return Empty{}, fail.Wrap(fail.New(fmt.Sprintf("Invalid arguments expected: %d, %v", 1, args)), fail.WithCode("Invalid arguments"))
			}
			return Empty{}, e.esBaseClient.DeleteIndex(ctx, args[0])
		case "copy":
			if len(args) != 2 {
				return Empty{}, fail.Wrap(fail.New(fmt.Sprintf("Invalid arguments expected: %d, %v", 2, args)), fail.WithCode("Invalid arguments"))
			}

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
			if len(args) != 1 {
				return Empty{}, fail.Wrap(fail.New(fmt.Sprintf("Invalid arguments expected: %d, %v", 1, args)), fail.WithCode("Invalid arguments"))
			}
			return e.esBaseClient.CountIndex(ctx, args[0])
		default:
			return Empty{}, fail.Wrap(fail.New(fmt.Sprintf("Invalid operation: %v", operation)), fail.WithCode("Invalid arguments"))
		}
	}

	if target == "mapping" {
		switch operation {
		case "get":
			if len(args) != 1 {
				return Empty{}, fail.Wrap(fail.New(fmt.Sprintf("Invalid arguments expected: %d, %v", 1, args)), fail.WithCode("Invalid arguments"))
			}
			return e.esBaseClient.GetMapping(ctx, args[0])
		case "update":
			// Thinking only alias case
			// Rethink when index
			// TODO think index case
			aliasName := args[0]
			mappingJSON := args[1]

			mapping, err := e.esBaseClient.GetMapping(ctx, args[0])
			if err != nil {
				return Empty{}, fail.Wrap(err)
			}
			indexMappings := map[string]interface{}{}
			err = json.Unmarshal([]byte(mapping), &indexMappings)
			if err != nil {
				return Empty{}, fail.Wrap(err)
			}
			if len(indexMappings) != 1 {
				return Empty{}, fail.New("Support only 1-alias 1-index case")
			}

			var oldIndexName string
			for k := range indexMappings {
				oldIndexName = k
			}

			newIndexName := aliasName + time.Now().Format("_20060102_150405")
			err = e.esBaseClient.CreateIndex(ctx, newIndexName, mappingJSON)
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
		case "create":
			if len(args) != 2 {
				return Empty{}, fail.Wrap(fail.New(fmt.Sprintf("Invalid arguments expected: %d, %v", 2, args)), fail.WithCode("Invalid arguments"))
			}
			return Empty{}, e.esBaseClient.CreateAlias(ctx, args[0], args[1])
		case "drop":
			if len(args) != 2 {
				return Empty{}, fail.Wrap(fail.New(fmt.Sprintf("Invalid arguments expected: %d, %v", 2, args)), fail.WithCode("Invalid arguments"))
			}
			// TODO implement
			return Empty{}, nil
			// return Empty{}, e.esBaseClient.DropAlias(ctx, args[0], args[1])
		case "add":
			if len(args) < 2 {
				return Empty{}, fail.Wrap(fail.New(fmt.Sprintf("Invalid arguments expected: %d, %v", 2, args)), fail.WithCode("Invalid arguments"))
			}
			return Empty{}, e.esBaseClient.AddAlias(ctx, args[0], args[1])
		case "remove":
			if len(args) < 2 {
				return Empty{}, fail.Wrap(fail.New(fmt.Sprintf("Invalid arguments expected: >= %d, %v", 2, args)), fail.WithCode("Invalid arguments"))
			}
			return Empty{}, e.esBaseClient.RemoveAlias(ctx, args[0], args[1:]...)
		default:
			return Empty{}, fail.Wrap(fail.New(fmt.Sprintf("Invalid operation: %v", operation)), fail.WithCode("Invalid arguments"))
		}
	}

	if target == "task" {
		switch operation {
		case "list":
			if len(args) != 0 {
				return Empty{}, fail.Wrap(fail.New(fmt.Sprintf("Invalid arguments expected: %d, %v", 0, args)), fail.WithCode("Invalid arguments"))
			}
			return e.esBaseClient.ListTask(ctx)
		case "get":
			if len(args) != 1 {
				return Empty{}, fail.Wrap(fail.New(fmt.Sprintf("Invalid arguments expected: %d, %v", 1, args)), fail.WithCode("Invalid arguments"))
			}
			return e.esBaseClient.GetTask(ctx, args[0])
		default:
			return Empty{}, fail.Wrap(fail.New(fmt.Sprintf("Invalid operation: %v", operation)), fail.WithCode("Invalid arguments"))
		}
	}

	return Empty{}, fail.Wrap(fail.New(fmt.Sprintf("Invalid target and operation %v %v", operation, target)), fail.WithCode("Invalid arguments"))
}
