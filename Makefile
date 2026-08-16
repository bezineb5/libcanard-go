# Makefile for Cy library Go migration
# Simple targets: help, build, test

.PHONY: help build test

# =============================================================================
# Main Targets
# =============================================================================

all: test ## Run all tests (default)

help: ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: build_cy build_can build_udp build_libcanard build_dsdl build_uavcan ## Build all packages

build_cy: ## Build the cy package
	cd cy && go build -v ./...

build_can: ## Build the CAN platform
	cd cy/can && go build -v ./...

build_udp: ## Build the UDP platform
	cd cy/udp && go build -v ./...

build_libcanard: ## Build the root libcanard package
	go build .

build_dsdl: ## Build the DSDL marshal package
	go build ./dsdl

build_uavcan: ## Build the generated uavcan types
	go build ./uavcan/...

test: test_cy test_can test_udp test_cavl test_olga test_libcanard test_dsdl test_uavcan ## Run all tests

test_cy: ## Run cy package tests
	cd cy && go test -v -timeout 30s ./...

test_can: ## Run CAN platform tests
	cd cy/can && go test -v -timeout 30s ./...

test_udp: ## Run UDP platform tests
	cd cy/udp && go test -v -timeout 30s ./...

test_cavl: ## Run CAVL tree tests
	cd cy/cavl && go test -v -timeout 30s ./tests/...

test_olga: ## Run Olga scheduler tests
	cd cy/olga && go test -v -timeout 30s ./tests/...

test_libcanard: ## Run root libcanard package tests
	go test -timeout 60s .

test_dsdl: ## Run DSDL marshal package tests
	go test -timeout 60s ./dsdl

test_uavcan: ## Run uavcan generated types tests (build-only; no _test.go)
	go test -timeout 60s ./uavcan/...
