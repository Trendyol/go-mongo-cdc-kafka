.PHONY: help
help:
	@echo "Available targets:"
	@echo "  make build        - Build the project"
	@echo "  make test         - Run tests"
	@echo "  make lint         - Run linter"
	@echo "  make fmt          - Format code"
	@echo "  make clean        - Clean build artifacts"
	@echo "  make example      - Run simple example"

.PHONY: build
build:
	go build -v ./...

.PHONY: test
test:
	go test -v -race -coverprofile=coverage.out ./...

.PHONY: lint
lint:
	golangci-lint run

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: clean
clean:
	go clean
	rm -f coverage.out

.PHONY: example
example:
	go run example/simple/main.go

.PHONY: deps
deps:
	go mod download
	go mod tidy

