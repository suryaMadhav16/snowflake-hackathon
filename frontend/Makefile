# Variables
SERVICE_NAME = frontend
DEV_IMAGE = snowflake-$(SERVICE_NAME):dev
PROD_IMAGE = snowflake-$(SERVICE_NAME):prod
DEV_CONTAINER = snowflake-$(SERVICE_NAME)-dev
PROD_CONTAINER = snowflake-$(SERVICE_NAME)-prod
DEV_PORT = 8501
PROD_PORT = 8502

# Build only commands
.PHONY: build-dev build-prod
build-dev:
	docker build --target development -t $(DEV_IMAGE) .

build-prod:
	docker build --target production -t $(PROD_IMAGE) .

# Development commands
.PHONY: dev
dev: build-dev
	docker run -d \
		--name $(DEV_CONTAINER) \
		-p $(DEV_PORT):8501 \
		-v $(PWD):/app \
		-v $(PWD)/.streamlit:/app/.streamlit \
		--env-file .env.dev \
		$(DEV_IMAGE)
	@echo "Development server running on http://localhost:$(DEV_PORT)"

# Production commands
.PHONY: prod
prod: build-prod
	docker run -d \
		--name $(PROD_CONTAINER) \
		-p $(PROD_PORT):8501 \
		-v $(PWD)/.streamlit:/app/.streamlit \
		--env-file .env.prod \
		$(PROD_IMAGE)
	@echo "Production server running on http://localhost:$(PROD_PORT)"

# Container management
.PHONY: dev-stop prod-stop dev-logs prod-logs dev-shell prod-shell status clean rebuild-dev rebuild-prod
dev-stop:
	docker stop $(DEV_CONTAINER) || true
	docker rm $(DEV_CONTAINER) || true

prod-stop:
	docker stop $(PROD_CONTAINER) || true
	docker rm $(PROD_CONTAINER) || true

clean: dev-stop prod-stop
	docker rmi $(DEV_IMAGE) || true
	docker rmi $(PROD_IMAGE) || true

dev-logs:
	docker logs -f $(DEV_CONTAINER)

prod-logs:
	docker logs -f $(PROD_CONTAINER)

dev-shell:
	docker exec -it $(DEV_CONTAINER) /bin/bash

prod-shell:
	docker exec -it $(PROD_CONTAINER) /bin/bash

status:
	@echo "Development Container Status:"
	@docker ps --filter "name=$(DEV_CONTAINER)" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
	@echo "\nProduction Container Status:"
	@docker ps --filter "name=$(PROD_CONTAINER)" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

rebuild-dev: dev-stop build-dev
	make dev

rebuild-prod: prod-stop build-prod
	make prod

# Help
.PHONY: help
help:
	@echo "Available commands:"
	@echo "\nBuild Commands:"
	@echo "  make build-dev         - Build development image only"
	@echo "  make build-prod        - Build production image only"
	@echo "\nDevelopment:"
	@echo "  make dev               - Start development environment"
	@echo "  make dev-stop          - Stop development container"
	@echo "  make dev-logs          - View development logs"
	@echo "  make dev-shell         - Access development shell"
	@echo "  make rebuild-dev       - Rebuild development environment"
	@echo "\nProduction:"
	@echo "  make prod              - Start production environment"
	@echo "  make prod-stop         - Stop production container"
	@echo "  make prod-logs         - View production logs"
	@echo "  make prod-shell        - Access production shell"
	@echo "  make rebuild-prod      - Rebuild production environment"
	@echo "\nUtilities:"
	@echo "  make status            - Show container status"
	@echo "  make clean             - Remove containers and images"