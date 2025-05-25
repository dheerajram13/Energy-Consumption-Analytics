.PHONY: help install test lint format check-style run build up down logs clean

# Define variables
DOCKER_COMPOSE_DEV=docker-compose -f docker-compose.yml
DOCKER_COMPOSE_PROD=docker-compose -f docker-compose.prod.yml

# Show help
help:
	@echo "Available commands:"
	@echo "  make install         Install development dependencies"
	@echo "  make test            Run tests"
	@echo "  make lint            Run linter"
	@echo "  make format          Format code"
	@echo "  make check-style     Check code style"
	@echo "  make run             Run the application locally"
	@echo "  make build           Build Docker images"
	@echo "  make up              Start all services"
	@echo "  make down            Stop all services"
	@echo "  make logs            View logs"
	@echo "  make clean           Clean up"

# Install development dependencies
install:
	pip install -r requirements.txt

# Run tests
test:
	pytest tests/

# Run linter
lint:
	black --check src/ tests/
	flake8 src/ tests/

# Format code
format:
	black src/ tests/
	autopep8 --in-place --recursive src/ tests/

# Check code style
check-style:
	black --check src/ tests/
	flake8 src/ tests/
	mypy src/ tests/

# Run the application locally
run:
	uvicorn src.main:app --reload --host 0.0.0.0 --port 8000

# Docker commands
build:
	$(DOCKER_COMPOSE_DEV) build

up:
	$(DOCKER_COMPOSE_DEV) up -d

down:
	$(DOCKER_COMPOSE_DEV) down

logs:
	$(DOCKER_COMPOSE_DEV) logs -f

# Production commands
prod-build:
	$(DOCKER_COMPOSE_PROD) build

prod-up:
	$(DOCKER_COMPOSE_PROD) up -d

prod-down:
	$(DOCKER_COMPOSE_PROD) down

prod-logs:
	$(DOCKER_COMPOSE_PROD) logs -f

# Clean up
clean:
	find . -type d -name "__pycache__" -exec rm -r {} +
	find . -type d -name ".pytest_cache" -exec rm -r {} +
	find . -type d -name ".mypy_cache" -exec rm -r {} +
	rm -rf .coverage htmlcov/
