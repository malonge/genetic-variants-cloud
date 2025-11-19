# Genetic Variants Cloud Pipeline

> Scalable ETL pipeline for transforming VCF genetic variant data into BigQuery using Apache Airflow and Docker.

## What It Does

Streams VCF files from local/remote sources → Transforms to Parquet → Loads to BigQuery for analytics. Built with production-grade patterns: streaming architecture, Docker isolation, comprehensive testing, and cloud-agnostic design.

**Current Status**: Phase 1 complete (VCF streaming abstraction). Next up: sharding, transformation, BigQuery loading.

## Quick Start

```bash
# Install dependencies
make install

# Run tests
make test-unit

# Build and start Airflow
make build
make up

# Access Airflow UI
open http://localhost:8080  # Login: admin/admin

# Trigger the pipeline
docker-compose exec airflow-scheduler airflow dags trigger vcf_pipeline
```

**Note**: Add a test VCF file to `test_data/sample.vcf.gz` before running.

## Testing

```bash
# All unit tests
make test-unit

# With coverage
poetry run pytest tests/unit/ --cov=src

# Linting
make lint
```

**Coverage**: Data models, streaming (local/HTTPS), factory pattern, error handling, resource cleanup.

## Architecture

```
Airflow Scheduler
    ↓
DockerOperator spawns Pipeline Worker Container
    ↓
VCFStreamer (abstract) → LocalVCFStreamer | HttpsVCFStreamer
    ↓
Streams VariantRecord objects → Future: Shard → Transform → Load to BigQuery
```

**Key Patterns**:
- Abstract interface decoupled from pysam implementation
- Factory pattern for URI-based streamer selection
- Docker-first for scalability and isolation

## Project Structure

```
├── dags/                  # Airflow DAG definitions
├── src/
│   ├── models.py          # VariantRecord dataclass
│   ├── streaming/         # VCF streaming implementations
│   └── tasks/             # Airflow task logic
├── tests/
│   ├── unit/              # Unit tests with mocks
│   └── integration/       # End-to-end tests
├── Dockerfile.airflow     # Airflow image
├── Dockerfile.pipeline    # Pipeline worker image
└── docker-compose.yml     # Service orchestration
```

## Tech Stack

Python 3.11 • Apache Airflow 2.8 • Docker • pysam • pytest • Poetry

---

**Built by Michael Alonge** | [LinkedIn](https://www.linkedin.com/in/michael-alonge/) | [GitHub](https://github.com/malonge)
