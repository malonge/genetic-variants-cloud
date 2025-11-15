# Makefile for VCF Pipeline Project

.PHONY: help install build up down test test-unit test-integration lint clean logs

help:
	@echo "Available commands:"
	@echo "  make install            - Install dependencies with Poetry"
	@echo "  make build              - Build Docker images"
	@echo "  make up                 - Start all services (Airflow, Postgres)"
	@echo "  make down               - Stop all services"
	@echo "  make test               - Run all tests"
	@echo "  make test-unit          - Run unit tests only"
	@echo "  make test-integration   - Run integration tests"
	@echo "  make lint               - Run linter (ruff)"
	@echo "  make logs               - View Airflow logs"
	@echo "  make clean              - Clean up containers and volumes"

# Install dependencies
install:
	@echo "Installing dependencies with Poetry..."
	poetry install

# Build Docker images
build:
	@echo "Building pipeline worker image..."
	docker build -t genetic-variants-pipeline:latest -f Dockerfile.pipeline .
	@echo "Building Airflow image..."
	docker-compose build

# Start services
up:
	docker-compose up -d
	@echo "⏳ Waiting for services to be healthy..."
	@sleep 15
	@echo "✅ Airflow UI available at http://localhost:8080"
	@echo "   Login: admin / admin"

# Stop services
down:
	docker-compose down

# Run all tests
test: test-unit

# Run unit tests
test-unit:
	@echo "Running unit tests with Poetry..."
	poetry run pytest tests/unit/ -v

# Run integration tests (requires test data)
test-integration:
	@echo "Running integration tests with Poetry..."
	poetry run pytest tests/integration/ -v

# Run linter
lint:
	@echo "Running ruff linter..."
	poetry run ruff check src/ tests/

# View logs
logs:
	docker-compose logs -f airflow-scheduler

# Clean everything
clean:
	docker-compose down -v
	docker rmi genetic-variants-pipeline:latest || true
	rm -rf data/*
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true

