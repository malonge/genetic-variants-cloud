"""
Genomics ELT Pipeline DAG

The data platform implements a Medallion Architecture (Bronze raw ingestion -> Silver staging & intermediate normalization -> Gold analytics-ready data marts):
1. Extract remote VCF variants directly to Bronze partitioned Parquet via PyArrow
2. Ingest 3,202 IGSR sample pedigree/population metadata into Bronze
3. Run Silver and Gold dbt models with DuckDB
4. Execute dbt data quality tests
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

DEFAULT_VCF_SOURCE = (
    "https://s3-us-west-2.amazonaws.com/human-pangenomics/T2T/CHM13/assemblies/variants/"
    "1000_Genomes_Project/chm13v2.0/all_samples_3202/"
    "1KGP.CHM13v2.0.chr21.recalibrated.snp_indel.pass.vcf.gz"
)


def run_vcf_to_parquet(**context):
    """Python task to extract VCF directly to Bronze Parquet."""
    from src.tasks.vcf_to_parquet import vcf_to_parquet_task

    conf = context['dag_run'].conf or {}
    source = conf.get("vcf_source", DEFAULT_VCF_SOURCE)
    region = conf.get("region", "chr21:10000000-10050000")
    lines_per_shard = int(conf.get("lines_per_shard", 5000))
    limit = conf.get("limit")
    if limit is not None:
        limit = int(limit)
    storage_base_path = conf.get("storage_base_path", "/data")

    return vcf_to_parquet_task(
        source=source,
        region=region,
        lines_per_shard=lines_per_shard,
        limit=limit,
        storage_base_path=storage_base_path,
    )


def run_ingest_samples(**context):
    """Python task to ingest 1000 Genomes IGSR sample metadata."""
    from src.tasks.ingest_samples import ingest_sample_metadata_task

    conf = context['dag_run'].conf or {}
    storage_base_path = conf.get("storage_base_path", "/data")

    return ingest_sample_metadata_task(storage_base_path=storage_base_path)


with DAG(
    'genomics_elt_pipeline',
    default_args=default_args,
    description='Automated VCF -> Parquet -> DuckDB -> dbt ELT pipeline',
    schedule_interval=None,  # Manual trigger or event-driven
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['genomics', 'vcf', 'parquet', 'duckdb', 'dbt', 'elt'],
) as dag:

    extract_vcf_to_parquet = PythonOperator(
        task_id='extract_vcf_to_parquet',
        python_callable=run_vcf_to_parquet,
        provide_context=True,
    )

    ingest_sample_metadata = PythonOperator(
        task_id='ingest_sample_metadata',
        python_callable=run_ingest_samples,
        provide_context=True,
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt_genomics && dbt run --profiles-dir .',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt_genomics && dbt test --profiles-dir .',
    )

    [extract_vcf_to_parquet, ingest_sample_metadata] >> dbt_run >> dbt_test
