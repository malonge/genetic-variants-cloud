# An Airflow ETL Pipeline for Transforming 1000 Genomes Variant Data into BigQuery for Scalable Analysis

## **Overview**

This project builds a complete data engineering pipeline for transforming raw genetic variant data (VCF) into an analytical data warehouse format (Parquet → BigQuery). The purpose is to demonstrate scalable data engineering design patterns for biological data while maintaining strong data modeling and orchestration practices.

The pipeline processes the **1000 Genomes Project, chr21**, recalled on **T2T-CHM13v2.0**.  
 Source: [1KGP.CHM13v2.0.chr21.recalibrated.snp\_indel.pass.vcf.gz](https://s3-us-west-2.amazonaws.com/human-pangenomics/T2T/CHM13/assemblies/variants/1000_Genomes_Project/chm13v2.0/all_samples_3202/1KGP.CHM13v2.0.chr21.recalibrated.snp_indel.pass.vcf.gz)

The pipeline uses Airflow to orchestrate streaming, sharding, and transformation tasks. Data is converted from VCF to TSV to Parquet, and then prepared for analytical storage in **BigQuery**. The design prioritizes idempotency, modularity, and cloud portability. The project is inspired and influenced by [The GCP Variant Transform tool](https://github.com/googlegenomics/gcp-variant-transforms), a part of the Google Genomics API. 

## 

## **Objectives**

1. **Transform bioinformatics data into analytics-ready format**

   * Standardize complex, nested VCF data into a relational schema suitable for SQL-based analysis.

   * Focus on interpretable fields relevant to variant quality and population frequency.

2. **Demonstrate scalable orchestration patterns**

   * Use dynamic task mapping in Airflow to process file shards asynchronously.

   * Model a workflow that scales naturally to cloud environments.

3. **Enable analytical workloads in BigQuery**

   * Define table partitioning and clustering strategies that optimize genomic queries.

   * Show how Parquet data produced by this pipeline can be efficiently ingested and queried in BigQuery.

4. **Ensure reproducibility and idempotency**

   * Make every run deterministic and append-only.

   * Allow reprocessing without overwriting previous results.

## 

## **Rationale and Design Philosophy**

VCF files are optimized for sequential variant storage, not analytics. They are large, line-based, and deeply nested with metadata in the INFO and FORMAT fields. Querying across cohorts or genomic regions requires a columnar format and indexed storage.

The transformation to Parquet enables:

* Efficient projection (querying only necessary fields)

* Compression and predicate pushdown

* Easy ingestion into analytical databases like BigQuery

* Partitioning and clustering along biologically meaningful dimensions (chromosome, position, allele frequency)

This pipeline demonstrates how a biologically motivated data format (VCF) can be integrated into modern data infrastructure without losing interpretability or scalability.

The pipeline is heavily influenced by the GCP Variant Transform project. The process of sharding variant data and converting to avro files is basically exactly what is done in GCP Variant Transform. The main difference here is to build a more general purpose tool that is not tightly coupled to Google Cloud APIs, especially since the Life Sciences/Genomics API is deprecated. The way we have designed this project, the pipeline works equally well locally, in GCP, in AWS, or anywhere. 

## **Architecture**

### **Workflow Summary**

**Stream → Shard → Transform → Load**

1. **Stream and Shard**

   * Stream VCF from a remote source.

   * Maintain the header in memory and write shards of fixed line counts.

   * Each shard is independently processable and immediately eligible for downstream transformation.

2. **VCF to TSV**

   * Flatten key variant attributes (e.g., CHROM, POS, REF, ALT, QUAL, AF, GT, GQ, PL).

   * Normalize complex or multi-allelic records where possible.

   * Preserve referential consistency between shards.

3. **TSV to Avro**

   * Convert tabular data into Avro using consistent schema.

   * Assign appropriate data types (e.g., integer for coordinates, float for quality metrics).

   * Produce one Avro file per shard, ready for loading into BigQuery.

4. **Load to BigQuery**

   * Stage Avro files in cloud storage (e.g., GCS).

   * Ingest into BigQuery table partitioned by genomic coordinate and clustered by variant attributes.

   * Validate row counts, schema alignment, and performance characteristics.

### Streaming Input Data

The first stage of the workflow streams genetic variant data and shards it into multiple non-overlapping files. The input can be provided as either a local file path or a remote HTTPS URL. This flexibility allows the pipeline to operate without ever downloading large monolithic VCFs to local storage. Instead, data are read as a stream and partitioned incrementally into smaller, self-contained shards.

In a cloud-orchestrated context, this approach enables predictable resource allocation. Each task operates with a fixed memory and disk footprint, regardless of input file size, which helps control cost and reduces operational risk. To start, the implementation will support **streaming via HTTPS**, but the design allows easy extension to other protocols (e.g., GCS, S3, or FTP).

#### **Implementation**

Streaming and variant parsing are implemented using **pysam**, the Python interface to **htslib**. `pysam` natively supports reading compressed VCF and BCF files directly over HTTP or HTTPS using range requests. It handles decompression, header parsing, and region-based iteration internally, making it well suited for high-throughput pipelines. This allows the workflow to process extremely large remote datasets while preserving the semantics of the VCF format. This approach streams data sequentially, maintaining only the header and the current shard buffer in memory. It avoids loading the full dataset or building in-memory indices, ensuring constant resource usage regardless of file size.

#### **Sharding Logic**

Each shard must include its own copy of the original VCF header to remain a valid, standalone file. Two parameters control the sharding process:

* **Region** – Only variants overlapping the specified genomic interval are processed.

* **Max lines per shard** – The maximum number of variant records written to a shard.

The workflow first reads and stores the entire header in memory. It then appends variant lines to an in-memory buffer until either the maximum line count is reached or no further records remain. At that point, the header and buffered lines are written together as a **BCF shard** to persistent storage, along with a small **index file** for lookup and validation. The input VCF is assumed to be coordinate-sorted. If multiple regions are later supported, the resulting outputs would need to be merged and re-sorted to maintain global coordinate order. Pysam might handle this natively.

#### **Output Organization**

Shards are written under a **timestamped run directory** to support idempotency through an append-only convention. All pipeline parameters are recorded in a **manifest file** for reproducibility and auditing. Shard files are named sequentially (`shard_0000.bcf` … `shard_NNNN.bcf`) with zero-padded indices to preserve lexical ordering within the storage system. This structure guarantees reproducible, self-contained output artifacts that can be independently validated or reprocessed downstream.

 


## **Airflow Design**

### **DAG Structure**

vcf\_pipeline  
│  
├── stream\_and\_shard\_vcf  → returns list of shard file paths  
│  
├── vcf\_to\_tsv \[mapped dynamically\]  
│  
├── tsv\_to\_parquet \[mapped dynamically\]  
│  
└── load\_parquet\_to\_bigquery \[mapped dynamically\]

### **Execution Model**

* Single DAG leveraging **dynamic task mapping** for shard-level parallelism.

* Downstream tasks begin as soon as their respective upstream shard is written (asynchronous fan-out).

* Each task operates independently but adheres to a shared run context for idempotency and traceability.

### **Orchestration Rationale**

Dynamic task mapping was chosen because:

* The number of shards is unknown until runtime.

* Each shard can be processed independently and concurrently.

* It allows the pipeline to mimic streaming behavior while preserving DAG-level visibility.

## **Data Modeling and Analytics Design**

### **Schema Strategy**

Focus on a subset of biologically and analytically relevant fields. Final schema (TBD after data inspection) will likely include:

| Column | Description | Type |
| ----- | ----- | ----- |
| `chrom` | Chromosome | STRING |
| `pos` | Genomic coordinate | INTEGER |
| `ref` | Reference allele | STRING |
| `alt` | Alternate allele | STRING |
| `qual` | Variant quality score | FLOAT |
| `af` | Allele frequency | FLOAT |
| `gt` | Genotype | STRING |
| `gq` | Genotype quality | FLOAT |
| `pl` | Phred-scaled likelihood | FLOAT |

### **BigQuery Table Design**

**Partitioning**

* Range partitioned on `pos` (genomic coordinate).

* Alternatively, combined partitioning by chromosome and position buckets.

**Clustering**

* Clustered by (`chrom`, `ref`, `alt`) to improve variant lookup performance.

**Storage Layout**

bq\_dataset.variants\_partitioned  
├── partition\_field: pos  
└── cluster\_fields: chrom, ref, alt

**Benefits**

* Efficient retrieval of variants in genomic windows.

* Cost-effective query scanning.

* Optimized joins and aggregations by allele or quality filters.

## **Idempotency and File Organization**

Each pipeline run writes to a unique timestamped directory to ensure append-only semantics:

data/  
  2025-11-09\_18-00-00/  
    shards/  
      shard\_0001.vcf  
      shard\_0002.vcf  
    tsv/  
      shard\_0001.tsv  
      shard\_0002.tsv  
    avro/  
      shard\_0001.avro  
      shard\_0002.avro

Future improvement: content-based hashing for deterministic shard identification and deduplication across runs.

## **Storage Abstraction**

All file operations are routed through a `StorageHandler` interface.  
 This enables the same pipeline to run locally or in the cloud without modification.

**Responsibilities**

* Abstract file I/O (read/write/list)

* Handle persistence layer (local disk or GCS)

* Manage naming conventions and run metadata

Example implementations:

* `LocalStorageHandler` for development

* `GCSStorageHandler` for cloud runs

## **Implementation Notes**

* Airflow operators will use native Python functions wrapped with `@task` decorators.

* Polars or Pandas will handle TSV → Parquet conversion.

* For BigQuery loading, use the `GCSToBigQueryOperator` or `BigQueryInsertJobOperator`.

* Schema evolution should be managed explicitly (define schema JSON file).

* Logging and monitoring should capture record counts at each stage for validation.

## **Validation and Analytics Demonstration**

Once the first Parquet shard is produced, load it into BigQuery to validate:

SELECT chrom, COUNT(\*) AS variants  
FROM \`dataset.variants\_partitioned\`  
WHERE pos BETWEEN 10000000 AND 20000000  
GROUP BY chrom;

Demonstrate:

* Query performance on coordinate ranges

* Filtering by variant quality or frequency

* Aggregations by allele or genotype class

This confirms that the transformed dataset supports analytical queries typical in genomics research and population studies.

## **Future Enhancements**

* **Data Quality Checks:** Validate INFO fields, enforce schema conformity, and verify completeness.

* **Schema Evolution:** Add richer annotations or sample-level metrics as nested columns.

* **Distributed Execution:** Scale sharding and conversion across cloud runners (Batch, Dataflow).

* **Incremental Updates:** Track variant additions or metadata corrections via snapshot diffing.

* **Automated Lineage:** Use Airflow metadata or OpenLineage integration to track provenance from source VCF to BigQuery table.

## **Key Takeaways**

* The pipeline demonstrates how raw genomic data can be transformed into analytics-grade structured data.

* Parquet is an enabling format, not the end goal — the value lies in **queryable variant analytics** through BigQuery.

* Dynamic task mapping allows flexible, scalable orchestration consistent with modern cloud data engineering practices.

* The design emphasizes modularity and idempotency, allowing future extension to GCS and BigQuery without major architectural changes.

* This approach models how a real-world genomics data platform can standardize, scale, and democratize variant data access for analytical workflows.

