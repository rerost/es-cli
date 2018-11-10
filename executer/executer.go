package executer

import (
	"context"

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

func (e *executerImp) Run(ctx context.Context, operation string, target string, args Args) (Result, error) {
	if target == "index" {
		switch operation {
		case "list":
			return e.esBaseClient.ListIndex(ctx)
		case "create":
			if len(args) != 2 {
				return Empty{}, fail.New("Invalid arguments")
			}
			return Empty{}, e.esBaseClient.CreateIndex(ctx, args[0], args[1])
		case "delete":
			if len(args) != 1 {
				return Empty{}, fail.New("Invalid arguments")
			}
			return Empty{}, e.esBaseClient.DeleteIndex(ctx, args[0])
		case "copy":
			if len(args) != 2 {
				return Empty{}, fail.New("Invalid arguments")
			}
			return Empty{}, e.esBaseClient.CopyIndex(ctx, args[0], args[1])
		}
	}

	return Empty{}, fail.New("Invalid arguments")
}
