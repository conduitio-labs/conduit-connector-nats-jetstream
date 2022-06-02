.PHONY: build test lint

build:
	go build -o conduit-connector-nats cmd/nats/main.go

test:
	go test $(GOTEST_FLAGS) ./...

lint:
	golangci-lint run