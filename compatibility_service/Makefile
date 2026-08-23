.PHONY: build
build:
	@go build -o bin/client ./cmd/client
	@go build -o bin/orchestrator ./cmd/orchestrator

.PHONY: test
test:
	@go test ./...

.PHONY: clean
clean:
	@rm -rf bin
