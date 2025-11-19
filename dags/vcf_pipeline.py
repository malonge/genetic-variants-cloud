"""
VCF Processing Pipeline DAG

This DAG demonstrates streaming VCF data using the DockerOperator pattern.
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
    description='Stream and process VCF genetic variant data',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 11, 15),
    catchup=False,
    tags=['genomics', 'vcf', 'streaming'],
) as dag:

    stream_vcf = DockerOperator(
        task_id='stream_vcf',
        image='genetic-variants-pipeline:latest',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',

        command=[
            'python', '-m', 'src.tasks.stream_vcf',
            # Use Airflow variable/param for source file
            '{{ dag_run.conf.get("vcf_source", "/test_data/sample.vcf.gz") }}',
            '--limit', '{{ dag_run.conf.get("limit", "100") }}',
        ],

        mounts=[
            Mount(
                source='/Users/malonge/Repos/genetic-variants-cloud/data',
                target='/data',
                type='bind'
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
