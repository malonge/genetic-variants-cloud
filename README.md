# Genomic Data Platform: VCF → Parquet → DuckDB → dbt Core

[![CI](https://github.com/malonge/genetic-variants-cloud/actions/workflows/ci.yml/badge.svg)](https://github.com/malonge/genetic-variants-cloud/actions)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/)
[![dbt-duckdb](https://img.shields.io/badge/dbt--duckdb-1.7+-orange.svg)](https://github.com/duckdb/dbt-duckdb)
[![License: CC0](https://img.shields.io/badge/License-CC0_1.0-lightgrey.svg)](https://creativecommons.org/publicdomain/zero/1.0/)

An automated, modern **ELT (Extract, Load, Transform)** data pipeline for population-scale human genomics data. 

The pipeline extracts multi-sample Variant Call Format (VCF) data from the **1000 Genomes Project** (recalled on **T2T-CHM13v2.0** across 3,202 individuals), loads it into partitioned **Apache Parquet** with nested struct arrays, creates external tables in **DuckDB**, and transforms the data with **dbt Core** into analytics-ready data marts.

---

## 🧬 Architecture Overview

```
                      ┌───────────────────────────────────────┐
                      │ 1000 Genomes Remote VCF (AWS S3 HTTP) │
                      └──────────────────┬────────────────────┘
                                         │
                                         ▼ (HTTPS Range Stream via pysam)
                      ┌───────────────────────────────────────┐
                      │    Direct Streamer & Arrow Batcher    │
                      │  (Zero intermediate BCF / TSV disk I/O)│
                      └──────────────────┬────────────────────┘
                                         │
                                         ▼
                      ┌───────────────────────────────────────┐
                      │  Bronze: Partitioned Raw Parquet      │
                      │  - Genomic variants + nested samples  │
                      │  - Directory: data/bronze/variants/   │
                      └──────────────────┬────────────────────┘
                                         │
    ┌───────────────────────────┐        │
    │ IGSR Sample Metadata File ├────────┤
    │ (data/bronze/samples.tsv) │        │
    └───────────────────────────┘        │
                                         ▼
                      ┌───────────────────────────────────────┐
                      │ DuckDB Analytical Engine              │
                      │ (External Table over Parquet & TSV)   │
                      └──────────────────┬────────────────────┘
                                         │
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │ dbt Transformation Layer (dbt-duckdb)                                       │
 │                                                                             │
 │  [Staging]                                                                  │
 │   ├── stg_variants.sql (Cleaned types, SNV/Indel classification)             │
 │   └── stg_samples.sql  (Standardized sample IDs, sex, ancestry codes)      │
 │                                                                             │
 │  [Intermediate]                                                             │
 │   ├── int_variant_sample_genotypes.sql (Unnested genotype matrix with QC)   │
 │   ├── int_genomic_binned_stats.sql      (100kb genomic window aggregations) │
 │   └── int_population_allele_freqs.sql  (Allele frequencies by superpop)    │
 │                                                                             │
 │  [Marts / Serving]                                                          │
 │   ├── fct_genomic_bin_density.sql       (Density, heterozygosity rate/kb)   │
 │   ├── fct_population_differentiation.sql (Cross-population allele spread)    │
 │   └── fct_variant_qc_metrics.sql        (Ti/Tv ratios, GQ/DP calibration)   │
 └─────────────────────────────────────────────────────────────────────────────┘
                                         │
                                         ▼
                      ┌───────────────────────────────────────┐
                      │ Downstream Analytics & BI Dashboards  │
                      │ (DuckDB / Metabase / Evidence / BI)   │
                      └──────────────────┬────────────────────┘
```

---

## 🎯 Key Data Engineering Highlights

1. **Direct VCF → Arrow → Parquet Extraction**:
   * Leverages `pysam` / `htslib` HTTP range requests to stream multi-gigabyte remote VCFs without downloading monolithic files to disk.
   * Batches parsed variant records directly into `pyarrow.Table` objects and serializes directly to Parquet with ZSTD compression—eliminating costly intermediate BCF or TSV conversions.
2. **Semi-Structured Nested Columnar Modeling**:
   * Retains the 3,202 multi-sample genotype calls in nested struct arrays (`LIST<STRUCT<sample_id, gt, gq, dp>>`) in the Bronze layer.
   * Prevents explosive row-multiplication during extraction while allowing DuckDB and dbt to selectively unnest and aggregate genotypes downstream.
3. **dbt Medallion Architecture**:
   * **Staging (`stg_`)**: Normalizes types, handles nulls, and standardizes genotype calls (`0/0` $\rightarrow$ Hom-Ref, `0/1` $\rightarrow$ Het, `1/1` $\rightarrow$ Hom-Alt).
   * **Intermediate (`int_`)**: Windowing, coordinate binning (`floor(pos / 100000) * 100000`), and population demographic joining.
   * **Marts (`fct_`)**: Dimensional fact tables ready for reporting:
     * `fct_genomic_bin_density`: Heterozygosity rate, SNV/Indel density per 100kb window.
     * `fct_population_differentiation`: Superpopulation allele frequencies (`EUR`, `AFR`, `AMR`, `EAS`, `SAS`) and frequency divergence.
     * `fct_variant_qc_metrics`: Transition vs. Transversion (Ti/Tv) quality calibration curves.
4. **Data Contracts & Automated Testing**:
   * Built-in schema validation (`unique`, `not_null`, `accepted_values`) and custom genomic range tests.
5. **Airflow Orchestration**:
   * Production DAG executing the complete flow: extract $\rightarrow$ ingest metadata $\rightarrow$ `dbt run` $\rightarrow$ `dbt test` $\rightarrow$ `dbt docs generate`.

---

## 📊 Datasets & Open Access Verification

* **Genotype Callset**: 1000 Genomes Project chr21 high-coverage calls on T2T-CHM13v2.0 ([Public Domain CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/)).
* **Cohort Metadata**: IGSR 3,202 sample pedigree & ancestry table from EMBL-EBI ([Open Access](https://www.internationalgenome.org/faq/do-i-need-permission-to-use-igsr-data-in-my-own-scientific-research/)).

---

## 🚀 Quick Start

### 1. Installation
```bash
# Install Python dependencies via Poetry
make install
```

### 2. Run Pipeline (Extract VCF to Bronze Parquet)
```bash
# Streams a slice of chr21 directly to partitioned Parquet and fetches IGSR metadata
make pipeline-run
```

### 3. Run dbt Transformations & Data Tests
```bash
# Execute staging, intermediate, and fact mart transformations in DuckDB
make dbt-run

# Run data quality tests
make dbt-test

# Serve interactive dbt documentation and lineage graph
make dbt-docs
```

### 4. Airflow Local Deployment
```bash
make up
# Access UI at http://localhost:8080 (admin / admin)
```

---

**Built by Michael Alonge** | [LinkedIn](https://www.linkedin.com/in/michael-alonge/) | [GitHub](https://github.com/malonge)
