.PHONY: build test lint

VERSION=$(shell git describe --tags --dirty --always)

all: lint test

build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-nats-jetstream.version=${VERSION}'" -o conduit-connector-nats-jetstream cmd/connector/main.go

test:
	docker-compose -f test/docker-compose.yml up --quiet-pull -d
	go test -race $(GOTEST_FLAGS) ./...; ret=$$?; \
		docker-compose -f test/docker-compose.yml down; \
		exit $$ret

lint:
	golangci-lint run
