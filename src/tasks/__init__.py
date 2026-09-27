"""Airflow task implementations."""
__all__ = ["vcf_to_parquet_task", "ingest_sample_metadata_task"]


def __getattr__(name):
    if name == "vcf_to_parquet_task":
        from .vcf_to_parquet import vcf_to_parquet_task
        return vcf_to_parquet_task
    elif name == "ingest_sample_metadata_task":
        from .ingest_samples import ingest_sample_metadata_task
        return ingest_sample_metadata_task
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
