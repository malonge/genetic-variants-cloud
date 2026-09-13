# Technical Design: Automated Genomic ELT Pipeline (VCF → Parquet → DuckDB → dbt)

## 1. Executive Summary & Project Vision

This project establishes a production-grade, end-to-end **ELT (Extract, Load, Transform)** data pipeline for large-scale population genomics data. Starting from raw multi-sample Variant Call Format (VCF) files, the pipeline extracts genomic variants into partitioned columnar storage (Apache Parquet), establishes external tables in an embedded analytical OLAP engine (DuckDB), models and curates data using modern analytics engineering standards (dbt Core), and orchestrates runs via Apache Airflow.

The primary objective is to demonstrate senior data engineering competencies—such as efficient streaming ingestion, columnar data architecture, nested semi-structured data modeling, modular transformations, automated testing, and reproducible orchestration—applied to high-throughput biomedical data.

---

## 2. Source Datasets & Licensing Verification

Both datasets used in this pipeline are open-access, publicly hosted, and verified to be free of usage restrictions:

1. **Variant Callset**:
   * **Dataset**: 1000 Genomes Project high-coverage whole-genome sequencing (3,202 samples, chromosome 21), recalled on the complete human reference genome **T2T-CHM13v2.0**.
   * **Source**: Telomere-to-Telomere (T2T) Consortium / Human Pangenome Reference Consortium (HPRC) on AWS Open Data:
     `https://s3-us-west-2.amazonaws.com/human-pangenomics/T2T/CHM13/assemblies/variants/1000_Genomes_Project/chm13v2.0/all_samples_3202/1KGP.CHM13v2.0.chr21.recalibrated.snp_indel.pass.vcf.gz`
   * **License / Terms**: **Public Domain ([CC0 1.0 Universal](https://creativecommons.org/publicdomain/zero/1.0/))**.
2. **Sample & Pedigree Metadata**:
   * **Dataset**: Official IGSR 3,202-sample pedigree and population indexing table.
   * **Source**: International Genome Sample Resource (IGSR) / EMBL-EBI:
     `https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/data_collections/1000G_2504_high_coverage/20130606_g1k_3202_samples_ped_population.txt`
   * **Attributes**: Family ID, Sample ID, Paternal ID, Maternal ID, Sex, Population (26 sub-populations), Superpopulation (`EUR`, `AFR`, `AMR`, `EAS`, `SAS`).
   * **License / Terms**: **Open Access (No restrictions)** under the Fort Lauderdale / Toronto agreement release guidelines.

---

## 3. High-Level Architecture & Data Flow

```
                      ┌───────────────────────────────────────┐
                      │ 1000 Genomes Remote VCF (AWS S3 HTTP) │
                      └──────────────────┬────────────────────┘
                                         │
                                         ▼ (HTTPS Range Stream via pysam)
                      ┌───────────────────────────────────────┐
                      │    Direct Streamer & Arrow Batcher    │
                      │  (No intermediate BCF or TSV writes)  │
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
                                         ▼
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
                      │ (Parquet / DuckDB / SQL Analytics)    │
                      └───────────────────────────────────────┘
```

---

## 4. Key Architectural Decisions & Rationale

### 4.1 Direct VCF → Arrow → Parquet (Eliminating Intermediate BCF/TSV)
* **Legacy approach**: Sharded VCF into intermediate BCF files, indexed them with `bcftools`, then planned a conversion from BCF to TSV and TSV to Parquet.
* **Overhaul approach**: Stream records directly using `pysam` over HTTPS into PyArrow `RecordBatch` / `Table` objects and write directly to Parquet with ZSTD compression.
* **Benefits**:
  * Reduces disk I/O by orders of magnitude (no intermediate uncompressed/compressed files).
  * Enforces typed Arrow schemas at write time without lossy string conversion.
  * Preserves exact VCF data types (floats, lists of alternate alleles, integers).

### 4.2 Nested Struct Arrays vs. Melted Rows in Bronze Parquet
* **Challenge**: 3,202 samples per variant record means melting during extraction produces $N \times 3,202$ rows (e.g. 10,000 variants $\rightarrow$ 32,020,000 rows), inflating file sizes and write latency.
* **Decision**: Maintain nested struct arrays for sample calls in the Bronze layer:
  ```
  samples: LIST<STRUCT<
      sample_id: STRING,
      gt: STRING,
      gq: INT32,
      dp: INT32
  >>
  ```
* **Benefits**:
  * Adheres strictly to the ELT pattern: raw data represents the source structure faithfully.
  * Drastically reduces row count and leverages Parquet dictionary encoding for repetitive sample IDs and genotypes.
  * DuckDB natively handles nested list-struct operations with zero serialization penalty (`UNNEST(samples)` or `list_aggregate`).

### 4.3 DuckDB as the Embedded Warehouse
* DuckDB operates directly on external Parquet files without requiring persistent server infrastructure.
* Supports out-of-core streaming execution, vectorized columnar execution, and rich list/struct SQL manipulation.
* Perfect for local reproducibility, CI testing, and cost-effective analytical querying without cloud warehouse vendor lock-in.

### 4.4 dbt Core (Medallion Architecture)
* **Staging (`stg_`)**: Clean column naming, cast data types, filter valid variants, standardize genotype encodings (`0/0` $\rightarrow$ Hom-Ref, `0/1` $\rightarrow$ Het, `1/1` $\rightarrow$ Hom-Alt).
* **Intermediate (`int_`)**: Unnest genotypes, join demographic and population metadata, compute genomic coordinate bin indices (`floor(pos / 100000) * 100000`), tag transition vs. transversion (Ti/Tv).
* **Marts (`fct_` / `dim_`)**: Highly denormalized, ready-to-query analytical tables optimized for dashboards and population genetics reporting.

---

## 5. dbt Data Modeling & Analytical Showcase

The dbt layer showcases three distinct analytical models demonstrating deep SQL and data engineering prowess:

1. **`fct_genomic_bin_density` (Genomic Distribution Mart)**:
   * **Grain**: Genomic window (e.g., 100kb bin on chromosome 21).
   * **Metrics**: Total variants, SNV count, Indel count, variant density (variants / kb), mean heterozygosity rate, mean transition/transversion ratio.
   * **Use case**: Mapping variation hotspots, centromeric gaps, and mutational load across chromosomes.
2. **`fct_population_differentiation` (Population Genetics Mart)**:
   * **Grain**: Variant ID $\times$ Superpopulation (`EUR`, `AFR`, `AMR`, `EAS`, `SAS`).
   * **Metrics**: Sample count, allele count (AC), total allele number (AN), calculated allele frequency (AF), observed heterozygosity rate.
   * **Use case**: Identifying ancestry-informative markers and population-stratified frequency divergence.
3. **`fct_variant_qc_metrics` (Sequencing QC & Calibration Mart)**:
   * **Grain**: Variant quality bracket $\times$ Variant type.
   * **Metrics**: Transition count ($A \leftrightarrow G$, $C \leftrightarrow T$), transversion count, empirical Ti/Tv ratio, average sequencing depth (`DP`), average genotype quality (`GQ`).
   * **Use case**: Validating callset reliability (human genome-wide Ti/Tv expected around 2.0–2.1; lower values in low-QUAL bins flag false positives).

---

## 6. Testing, Quality Assurance & Orchestration

* **Unit Testing (`pytest`)**: Verify VCF streaming, Arrow batching, and schema formatting.
* **dbt Data Tests**:
  * Built-in tests: `unique`, `not_null`, `accepted_values`.
  * Custom generic tests: Coordinate boundary validation (`assert_valid_genomic_range`), frequency boundaries (`assert_in_range(0.0, 1.0)`).
* **Airflow Orchestration**:
  * End-to-end DAG orchestrating:
    1. Extract remote VCF to Bronze Parquet.
    2. Download & stage IGSR sample metadata.
    3. Run dbt models (`dbt run`).
    4. Run dbt tests (`dbt test`).
    5. Generate documentation and lineage (`dbt docs generate`).
