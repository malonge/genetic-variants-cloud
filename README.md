# Genetic Variants Cloud Pipeline

> Scalable ETL pipeline for transforming VCF genetic variant data into BigQuery using Apache Airflow and Docker.

## What It Does

Streams VCF files from local/remote sources → Transforms to Parquet → Loads to BigQuery for analytics. Built with production-grade patterns: streaming architecture, Docker isolation, comprehensive testing, and cloud-agnostic design.

## Quick Start

```bash
# Install dependencies
make install

# Run tests
make test-unit

# Run E2E tests
# Processes a portion of the 1000 Genomes Project, chr21, T2T-CHM13v2.0
make test-e2e

# Build and start Airflow
make build
make up

# Access Airflow UI
open http://localhost:8080  # Login: admin/admin
```

## Data Persistence

When running the pipeline locally the postgres database is persisted in a Docker volume called `postgres-db-volume` and the output variant data is persisted in a Docker volume called `pipeline-data`. This can be adjusted/customized in the `docker-compose.yml` file.

When running the pipeline in the cloud the persistent storage type can be configured. More details TBD.

---

**Built by Michael Alonge** | [LinkedIn](https://www.linkedin.com/in/michael-alonge/) | [GitHub](https://github.com/malonge)
