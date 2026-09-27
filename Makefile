# Makefile for Genomic ELT Pipeline Project (VCF -> Parquet -> DuckDB -> dbt)

.PHONY: help install pipeline-extract pipeline-samples pipeline-run dbt-run dbt-test dbt-docs test test-unit lint clean build up down logs

help:
	@echo "Available commands:"
	@echo "  make install            - Install Python dependencies via uv"
	@echo "  make pipeline-run       - Run extraction (VCF -> Parquet & IGSR metadata)"
	@echo "  make dbt-run            - Execute dbt transformations in DuckDB"
	@echo "  make dbt-test           - Run dbt schema and data quality tests"
	@echo "  make dbt-docs           - Generate dbt documentation"
	@echo "  make test               - Run all pytest test suites"
	@echo "  make lint               - Run ruff linter"
	@echo "  make build              - Build Docker containers"
	@echo "  make up                 - Start Airflow & PostgreSQL services"
	@echo "  make down               - Stop services"
	@echo "  make clean              - Clean local build artifacts and databases"

# Install dependencies
install:
	@echo "Installing dependencies with uv..."
	uv sync --all-groups

# Extract VCF slice directly to Bronze Parquet
pipeline-extract:
	@echo "Extracting VCF variants to Bronze Parquet..."
	uv run python -m src.tasks.vcf_to_parquet \
		"https://s3-us-west-2.amazonaws.com/human-pangenomics/T2T/CHM13/assemblies/variants/1000_Genomes_Project/chm13v2.0/all_samples_3202/1KGP.CHM13v2.0.chr21.recalibrated.snp_indel.pass.vcf.gz" \
		--region "chr21:10000000-10050000" \
		--lines-per-shard 5000 \
		--storage-base-path "data"

# Ingest IGSR sample pedigree metadata
pipeline-samples:
	@echo "Ingesting 1000 Genomes sample metadata..."
	uv run python -m src.tasks.ingest_samples --storage-base-path "data"

# Full extraction
pipeline-run: pipeline-extract pipeline-samples

# dbt transformations
dbt-run:
	@echo "Running dbt transformations in DuckDB..."
	cd dbt_genomics && uv run dbt run --profiles-dir .

# dbt data tests
dbt-test:
	@echo "Running dbt data quality tests..."
	cd dbt_genomics && uv run dbt test --profiles-dir .

# dbt docs
dbt-docs:
	@echo "Generating dbt documentation..."
	cd dbt_genomics && uv run dbt docs generate --profiles-dir .

# Run test suite
test: test-unit

test-unit:
	@echo "Running unit tests with uv..."
	uv run pytest tests/unit/ -v

# Run linter
lint:
	@echo "Running ruff linter..."
	uv run ruff check src/ tests/

# Docker build
build:
	@echo "Building Docker images..."
	docker compose build

# Start services
up:
	docker compose up -d
	@echo "Airflow UI available at http://localhost:8080 (admin / admin)"

# Stop services
down:
	docker compose down

# View logs
logs:
	docker compose logs -f airflow-scheduler

# Clean everything
clean:
	docker compose down -v 2>/dev/null || true
	rm -rf data/*.duckdb* data/bronze/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf dbt_genomics/target dbt_genomics/dbt_packages
