# Variables
DOCKER_COMPOSE_DEV = docker-compose -f docker-compose.dev.yml
DOCKER_COMPOSE_PROD = docker-compose -f docker-compose.prod.yml
DOCKER_HUB_USERNAME ?= udaykirandasari

# Image names
BACKEND_DEV_IMAGE = snowflake-backend:dev
BACKEND_PROD_IMAGE = snowflake-backend:prod
FRONTEND_DEV_IMAGE = snowflake-frontend:dev
FRONTEND_PROD_IMAGE = snowflake-frontend:prod

# Docker Hub repositories
BACKEND_REPO = $(DOCKER_HUB_USERNAME)/snowflake-backend
FRONTEND_REPO = $(DOCKER_HUB_USERNAME)/snowflake-frontend

# Platform targets
PLATFORMS = linux/amd64,linux/arm64

# Build only commands
.PHONY: build-dev build-prod
build-dev:
	$(DOCKER_COMPOSE_DEV) build

build-prod:
	$(DOCKER_COMPOSE_PROD) build

# Development commands
.PHONY: dev
dev:
	$(DOCKER_COMPOSE_DEV) up --build -d
	@echo "Development environment started:"
	@echo "Backend: http://localhost:8000"
	@echo "Frontend: http://localhost:8501"

.PHONY: dev-down
dev-down:
	$(DOCKER_COMPOSE_DEV) down

.PHONY: dev-logs
dev-logs:
	$(DOCKER_COMPOSE_DEV) logs -f

# Production commands
.PHONY: prod
prod:
	$(DOCKER_COMPOSE_PROD) up --build -d
	@echo "Production environment started:"
	@echo "Backend: http://localhost:8000"
	@echo "Frontend: http://localhost:8501"

.PHONY: prod-down
prod-down:
	$(DOCKER_COMPOSE_PROD) down

.PHONY: prod-logs
prod-logs:
	$(DOCKER_COMPOSE_PROD) logs -f

# Service-specific commands
.PHONY: dev-backend-logs dev-frontend-logs prod-backend-logs prod-frontend-logs
dev-backend-logs:
	$(DOCKER_COMPOSE_DEV) logs -f backend

dev-frontend-logs:
	$(DOCKER_COMPOSE_DEV) logs -f frontend

prod-backend-logs:
	$(DOCKER_COMPOSE_PROD) logs -f backend

prod-frontend-logs:
	$(DOCKER_COMPOSE_PROD) logs -f frontend

# Shell access
.PHONY: dev-backend-shell dev-frontend-shell prod-backend-shell prod-frontend-shell
dev-backend-shell:
	$(DOCKER_COMPOSE_DEV) exec backend /bin/bash

dev-frontend-shell:
	$(DOCKER_COMPOSE_DEV) exec frontend /bin/bash

prod-backend-shell:
	$(DOCKER_COMPOSE_PROD) exec backend /bin/bash

prod-frontend-shell:
	$(DOCKER_COMPOSE_PROD) exec frontend /bin/bash

# Docker Hub commands
.PHONY: docker-login docker-push docker-push-dev docker-push-prod
docker-login:
	docker login

docker-push-dev: docker-login
	docker tag $(BACKEND_DEV_IMAGE) $(BACKEND_REPO):dev
	docker push $(BACKEND_REPO):dev
	docker tag $(FRONTEND_DEV_IMAGE) $(FRONTEND_REPO):dev
	docker push $(FRONTEND_REPO):dev

docker-push-prod: docker-login
	docker tag $(BACKEND_PROD_IMAGE) $(BACKEND_REPO):prod
	docker push $(BACKEND_REPO):prod
	docker tag $(FRONTEND_PROD_IMAGE) $(FRONTEND_REPO):prod
	docker push $(FRONTEND_REPO):prod

docker-push: docker-push-dev docker-push-prod

docker-push-multi-arch-dev: docker-login
	@echo "Building and pushing multi-arch development images for platforms: $(PLATFORMS)"
	docker buildx create --use --name multi-arch-builder || true
	docker buildx build --platform $(PLATFORMS) \
		-f backend/Dockerfile \
		--target development \
		--push \
		-t $(BACKEND_REPO):dev \
		./backend
	docker buildx build --platform $(PLATFORMS) \
		-f frontend/Dockerfile \
		--target development \
		--push \
		-t $(FRONTEND_REPO):dev \
		./frontend
	docker buildx rm multi-arch-builder

docker-push-multi-arch-prod: docker-login
	@echo "Building and pushing multi-arch production images for platforms: $(PLATFORMS)"
	docker buildx create --use --name multi-arch-builder || true
	docker buildx build --platform $(PLATFORMS) \
		-f backend/Dockerfile \
		--target production \
		--push \
		-t $(BACKEND_REPO):prod \
		./backend
	docker buildx build --platform $(PLATFORMS) \
		-f frontend/Dockerfile \
		--target production \
		--push \
		-t $(FRONTEND_REPO):prod \
		./frontend
	docker buildx rm multi-arch-builder

docker-push-multi-arch: docker-push-multi-arch-dev docker-push-multi-arch-prod
	@echo "Multi-architecture images successfully built and pushed to Docker Hub"


# Utility commands
.PHONY: status clean
status:
	@echo "Development Containers:"
	@$(DOCKER_COMPOSE_DEV) ps
	@echo "\nProduction Containers:"
	@$(DOCKER_COMPOSE_PROD) ps

clean:
	$(DOCKER_COMPOSE_DEV) down -v --rmi all
	$(DOCKER_COMPOSE_PROD) down -v --rmi all

# Help
.PHONY: help
help:
	@echo "Available commands:"
	@echo "\nBuild Commands:"
	@echo "  make build-dev         - Build development images only"
	@echo "  make build-prod        - Build production images only"
	@echo "\nDevelopment:"
	@echo "  make dev               - Start development environment"
	@echo "  make dev-down          - Stop development environment"
	@echo "  make dev-logs          - View all development logs"
	@echo "  make dev-backend-logs  - View backend development logs"
	@echo "  make dev-frontend-logs - View frontend development logs"
	@echo "  make dev-backend-shell - Access backend development shell"
	@echo "  make dev-frontend-shell- Access frontend development shell"
	@echo "\nProduction:"
	@echo "  make prod              - Start production environment"
	@echo "  make prod-down         - Stop production environment"
	@echo "  make prod-logs         - View all production logs"
	@echo "  make prod-backend-logs - View backend production logs"
	@echo "  make prod-frontend-logs- View frontend production logs"
	@echo "  make prod-backend-shell- Access backend production shell"
	@echo "  make prod-frontend-shell- Access frontend production shell"
	@echo "\nDocker Hub:"
	@echo "  make docker-push-dev   - Push development images to Docker Hub"
	@echo "  make docker-push-prod  - Push production images to Docker Hub"
	@echo "  make docker-push       - Push all images to Docker Hub"
	@echo "\nUtilities:"
	@echo "  make status            - Show all container status"
	@echo "  make clean             - Remove all containers, networks, and images"