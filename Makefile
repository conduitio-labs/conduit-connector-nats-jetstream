.PHONY: build test lint

build:
	go build -o conduit-connector-nats-jetstream cmd/nats/main.go

test:
	docker-compose -f test/docker-compose.yml up --quiet-pull -d
	go test $(GOTEST_FLAGS) ./...; ret=$$?; \
		docker-compose -f test/docker-compose.yml down; \
		exit $$ret

lint:
	golangci-lint run