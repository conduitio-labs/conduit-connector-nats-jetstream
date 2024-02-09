VERSION=$(shell git describe --tags --dirty --always)

.PHONY: build
build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-nats-jetstream.version=${VERSION}'" -o conduit-connector-nats-jetstream cmd/connector/main.go

.PHONY: test
test:
	docker-compose -f test/docker-compose.yml up --quiet-pull -d
	go test -race $(GOTEST_FLAGS) ./...; ret=$$?; \
		docker-compose -f test/docker-compose.yml down; \
		exit $$ret

.PHONY: lint
lint:
	golangci-lint run

.PHONY: generate
generate:
	go generate ./...

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools.go
	@go list -e -f '{{ join .Imports "\n" }}' tools.go | xargs -tI % go install %
	@go mod tidy
