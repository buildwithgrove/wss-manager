SHELL := /bin/bash

build:
	CGO_ENABLED=0 GOOS=linux go build -a -o bin/wss-manager ./main.go

test:
	@echo "🛠 Running all tests..."
	go test -v ./...
	@echo "✅ All tests completed!"

test_unit:
	@echo "🛠 Running Unit tests..."
	@go test ./... -short || true
	@echo "✅ Unit tests completed!"

# This target install pre-commit to the repo and should be run only once, after cloning the repo for the first time.
init-pre-commit:
	wget https://github.com/pre-commit/pre-commit/releases/download/v2.20.0/pre-commit-2.20.0.pyz;
	python3 pre-commit-2.20.0.pyz install;
	go install golang.org/x/tools/cmd/goimports@v0.6.0;
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.57.1;
	go install -v github.com/go-critic/go-critic/cmd/gocritic@v0.11.0;
	python3 pre-commit-2.20.0.pyz run --all-files;
	rm pre-commit-2.20.0.pyz;
	rm pre-commit-2.20.0.pyz.*;
