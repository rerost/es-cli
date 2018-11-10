package executer

import (
	"context"

	"github.com/rerost/es-cli/infra/es"
)

type Result interface{
	String() string
}
type Args []string

type Executer interface {
	Run(ctx context.Context, operation string, target string, args Args) (Result, error)
}

type executerImp struct {
	esBaseClient es.BaseClient
}

func NewExecuter(esBaseClient es.BaseClient) Executer {
	return &executerImp{esBaseClient: esBaseClient}
}

func (e *executerImp) Run(ctx context.Context, operation string, target string, args Args) (Result, error) {
	if operation == "list" && target == "index" {
		if len(args) != 1 {
			return Result{}, fail.New("Invalid arguments")
		}
		return es.BaseClient.ListIndex(args[0])
	}
	return Result{}, nil
}
