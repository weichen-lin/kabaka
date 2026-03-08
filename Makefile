# Valkey settings
VALKEY_IMAGE = valkey/valkey:latest
VALKEY_CONTAINER = kabaka-valkey
VALKEY_PORT = 6379

.PHONY: help up down restart test test-verbose dash dash-build clean

help:
	@echo "Usage:"
	@echo "  make up      - Start Valkey container"
	@echo "  make down    - Stop and remove Valkey container"
	@echo "  make restart - Restart Valkey container"
	@echo "  make test    - Run all tests"
	@echo "  make dash    - Build frontend and run dashboard example"

up:
	@if [ ! "$$(docker ps -q -f name=$(VALKEY_CONTAINER))" ]; then \
		if [ "$$(docker ps -aq -f status=exited -f name=$(VALKEY_CONTAINER))" ]; then \
			docker start $(VALKEY_CONTAINER); \
		else \
			docker run -d --name $(VALKEY_CONTAINER) -p $(VALKEY_PORT):6379 $(VALKEY_IMAGE); \
		fi; \
	fi
	@echo "Valkey is up and running on port $(VALKEY_PORT)"

down:
	@docker stop $(VALKEY_CONTAINER) || true
	@docker rm $(VALKEY_CONTAINER) || true
	@echo "Valkey container stopped and removed"

restart: down up

test: up
	go test ./...

test-verbose: up
	go test -v ./...

dash-build:
	@echo "🏗️  Building Frontend with Bun..."
	@cd dashboard/frontend && bun run build
	@echo "📦 Syncing dist assets..."
	@rm -rf dashboard/dist && cp -r dashboard/frontend/dist dashboard/dist

dash: dash-build
	@echo "🚀 Starting Kabaka Dashboard..."
	@go run dashboard/examples/test_app.go

clean:
	@rm -f kabaka-test
	@rm -rf dashboard/dist
	@echo "🧹 Cleaned up temporary binaries and assets"
