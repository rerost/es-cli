PHONY: .all
all: es-cli

PHONY: vendor
vendor:
	go get -u github.com/golang/dep/cmd/dep
	dep ensure -v -vendor-only

PHONY: go-test
go-test: vendor
	@go test -v ./...

PHONY: e2e-test
e2e-test: es-cli
	./script/e2e-test


## Use this in CI
PHONY: es-cli
es-cli: vendor
	go build -o bin/es-cli .

PHONY: test
test: go-test e2e-test
