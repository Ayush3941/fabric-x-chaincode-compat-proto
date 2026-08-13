.PHONY: build
build:
	@go build -o bin/helper ./cmd/helper
	@go build -o bin/client ./cmd/client

.PHONY: test
test:
	@go test ./...

.PHONY: clean
clean:
	@rm -rf bin
