.PHONY: test bench clean docker-test

# Default target
all: test

# Run tests
test:
	go test -v -bench=. ./...

sim:
	go run .