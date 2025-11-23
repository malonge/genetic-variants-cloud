"""
VCF Processing Pipeline DAG

This DAG orchestrates the VCF processing pipeline:
1. Shard VCF data into compressed BCF files with indices
2. Convert BCF shards to TSV (future)
3. Convert TSV to Parquet (future)
4. Load Parquet to BigQuery (future)

Each task runs in an isolated container with the pipeline code and dependencies.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'vcf_pipeline',
    default_args=default_args,
    description='Stream, shard, and process VCF genetic variant data',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 11, 15),
    catchup=False,
    tags=['genomics', 'vcf', 'streaming', 'sharding'],
) as dag:

    shard_vcf = DockerOperator(
        task_id='shard_vcf',
        image='genetic-variants-pipeline:latest',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',

        command=[
            'python', '-m', 'src.tasks.shard_vcf',
            # Use Airflow variable/param for source file
            '{{ dag_run.conf.get("vcf_source", "/test_data/sample.vcf.gz") }}',
            '--lines-per-shard', '{{ dag_run.conf.get("lines_per_shard", "10000") }}',
            '--storage-type', 'local',
            '--storage-base-path', '/data',
        ],

        mounts=[
            Mount(
                source='genetic-variants-cloud_pipeline-data',
                target='/data',
                type='volume'
            ),
            Mount(
                source='/Users/malonge/Repos/genetic-variants-cloud/test_data',
                target='/test_data',
                type='bind',
                read_only=True
            ),
        ],

        mem_limit='2g',
        mount_tmp_dir=False,
    )
