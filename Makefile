# Pulse — top-level developer entrypoints.
# The ocb version MUST match the collector module versions in
# data-plane/builder-config.yaml (both currently v0.156.0).

OCB_VERSION ?= v0.156.0
GOBIN       := $(shell go env GOPATH)/bin
OCB         := $(GOBIN)/builder

.PHONY: help ocb tidy build run clean

help: ## List available targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-10s %s\n", $$1, $$2}'

ocb: ## Install the OpenTelemetry Collector Builder (pinned)
	go install go.opentelemetry.io/collector/cmd/builder@$(OCB_VERSION)

tidy: ## Tidy the custom processor module
	cd data-plane/processors/filterprocessor && go mod tidy

build: ## Compile the otelcol-pulse binary into data-plane/dist/
	cd data-plane && $(OCB) --config builder-config.yaml

run: ## Run the collector with the local dev pipeline
	./data-plane/dist/otelcol-pulse --config data-plane/config/otelcol-dev.yaml

clean: ## Remove ocb build output
	rm -rf data-plane/dist
