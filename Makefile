.PHONY: help
help:
	@echo "Available targets:"
	@echo "  make build        - Build the project"
	@echo "  make test         - Run tests"
	@echo "  make init         - Install golangci-lint and fieldalignment"
	@echo "  make linter       - Run linter and fieldalignment"
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

.PHONY: init
init:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.6.2
	go install golang.org/x/tools/go/analysis/passes/fieldalignment/cmd/fieldalignment@v0.39.0

.PHONY: linter
linter:
	fieldalignment -fix ./...
	golangci-lint run -c .golangci.yml --timeout=5m -v --fix

.PHONY: lint
lint:
	golangci-lint run -c .golangci.yml --timeout=5m -v

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: clean
clean:
	go clean
	rm -f coverage.out

.PHONY: example
example:
	go run example/complete-builder/main.go

.PHONY: deps
deps:
	go mod download
	go mod tidy

