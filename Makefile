SHELL=/bin/bash -o pipefail

$(eval RELEASE_TAG := $(shell cat version.json | jq ".version" --raw-output))

PHONY: .all
all: es-cli

PHONY: vendor
vendor:
	go get github.com/izumin5210/gex/cmd/gex
	go mod download

PHONY: generate
generate:
	go generate ./...

PHONY: go-test
go-test: vendor
	go test -v ./... | gex cgt

PHONY: e2e-test
e2e-test: build 
	./script/e2e-test

PHONY: clear
clear:
	rm -rf bin/
	rm -rf build/
	rm -rf vendor/

PHONY: install
install: vendor
	go install

## Use this in CI
PHONY: build
build: vendor
ifdef GOOS
	go build -o build/es-cli-$(GOOS)-$(GOARCH) $ .
else
	go build -o build/es-cli .
endif

PHONY: test
test: go-test e2e-test

PHONY: release
release: vendor build	
	gex ghr -t $(GITHUB_ACCESS_TOKEN) $(RELEASE_TAG) build/
