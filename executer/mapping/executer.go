package mapping

import "context"

type Mapping interface {
	UpdateMapping(ctx context.Context, aliasName string, mappingJSON string) error
}
