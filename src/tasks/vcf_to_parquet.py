"""
Task for streaming VCF data directly to Parquet using PyArrow.

Eliminates intermediate BCF and TSV conversions. Converts pysam.VariantRecord
directly into PyArrow Tables with nested sample structs, written to partitioned
Bronze Parquet files with ZSTD compression.
"""
import io
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from src.models import VARIANT_PARQUET_SCHEMA
from src.storage import create_storage
from src.streaming import create_streamer

logger = logging.getLogger(__name__)


def vcf_record_to_dict(record, sample_names: List[str]) -> Dict[str, Any]:
    """
    Extract a pysam.VariantRecord into a typed dictionary matching VARIANT_PARQUET_SCHEMA.
    """
    # Extract INFO fields
    info = record.info
    info_dp = info.get("DP")
    if isinstance(info_dp, (tuple, list)):
        info_dp = info_dp[0] if len(info_dp) > 0 else None

    info_ac = info.get("AC")
    if info_ac is None:
        info_ac_list = []
    elif isinstance(info_ac, (tuple, list)):
        info_ac_list = list(info_ac)
    else:
        info_ac_list = [info_ac]

    info_af = info.get("AF")
    if info_af is None:
        info_af_list = []
    elif isinstance(info_af, (tuple, list)):
        info_af_list = [float(x) for x in info_af]
    else:
        info_af_list = [float(info_af)]

    info_an = info.get("AN")
    if isinstance(info_an, (tuple, list)):
        info_an = info_an[0] if len(info_an) > 0 else None

    # Filter keys
    filter_keys = list(record.filter.keys()) if record.filter else []

    # Alts
    alts = list(record.alts) if record.alts else []

    # Nested sample calls: list of structs
    sample_structs = []
    record_samples = record.samples
    for s_name in sample_names:
        s_data = record_samples.get(s_name)
        if s_data is None:
            continue

        gt_tuple = s_data.get("GT")
        if gt_tuple is None or None in gt_tuple:
            gt_str = "./."
        else:
            is_phased = getattr(s_data, "phased", False)
            sep = "|" if is_phased else "/"
            gt_str = sep.join(str(allele) for allele in gt_tuple)

        gq = s_data.get("GQ")
        dp = s_data.get("DP")

        sample_structs.append({
            "sample_id": s_name,
            "gt": gt_str,
            "gq": int(gq) if gq is not None else None,
            "dp": int(dp) if dp is not None else None,
        })

    return {
        "chrom": str(record.chrom),
        "pos": int(record.pos),
        "id": str(record.id) if record.id else None,
        "ref": str(record.ref),
        "alt": alts,
        "qual": float(record.qual) if record.qual is not None else None,
        "filter": filter_keys,
        "info_dp": int(info_dp) if info_dp is not None else None,
        "info_ac": info_ac_list,
        "info_af": info_af_list,
        "info_an": int(info_an) if info_an is not None else None,
        "samples": sample_structs,
    }


def write_parquet_shard(
    records: List[Dict[str, Any]],
    shard_index: int,
    chrom: str,
    run_id: str,
    storage,
) -> str:
    """
    Serialize records to PyArrow Table and write Parquet shard to storage.
    """
    table = pa.Table.from_pylist(records, schema=VARIANT_PARQUET_SCHEMA)

    # Write to in-memory buffer
    sink = io.BytesIO()
    pq.write_table(
        table,
        sink,
        compression="zstd",
        compression_level=3,
        use_dictionary=True,
    )
    sink.seek(0)
    parquet_bytes = sink.getvalue()

    # Hive-partitioned directory layout: bronze/variants/chrom=chr21/shard_0000.parquet
    shard_filename = f"shard_{shard_index:04d}.parquet"
    rel_path = f"bronze/variants/chrom={chrom}/{shard_filename}"

    storage.write_file(parquet_bytes, rel_path)
    logger.info(f"Successfully wrote {len(records)} variants to {rel_path} ({len(parquet_bytes)} bytes)")
    return rel_path


def vcf_to_parquet_task(
    source: str,
    region: Optional[str] = None,
    lines_per_shard: int = 5000,
    limit: Optional[int] = None,
    storage_type: str = "local",
    storage_base_path: str = "data",
    run_id: Optional[str] = None,
) -> List[str]:
    """
    Stream VCF from local path or HTTPS URL and convert directly into Parquet shards.

    Args:
        source: VCF file path or HTTPS URL
        region: Genomic interval (e.g. 'chr21', 'chr21:10000000-10050000')
        lines_per_shard: Maximum records per Parquet shard
        limit: Optional maximum total variants to extract (for dev/testing)
        storage_type: 'local' or 'gcs'
        storage_base_path: Root output directory
        run_id: Run identifier

    Returns:
        List of relative paths to written Parquet files
    """
    if run_id is None:
        run_id = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    logger.info(f"Starting direct VCF -> Parquet extraction from {source}")
    logger.info(f"Region: {region or 'all'}, Shard size: {lines_per_shard}, Limit: {limit}")

    storage = create_storage(storage_type=storage_type, base_path=storage_base_path)
    streamer = create_streamer(source, region)

    written_shards = []
    record_buffer = []
    shard_idx = 0
    total_variants = 0

    with streamer:
        header = streamer.get_header()
        sample_names = list(header.samples)
        logger.info(f"Streaming VCF: {len(sample_names)} samples detected")

        for record in streamer.stream():
            row = vcf_record_to_dict(record, sample_names)
            record_buffer.append(row)
            total_variants += 1

            if len(record_buffer) >= lines_per_shard:
                chrom = record_buffer[0]["chrom"]
                shard_path = write_parquet_shard(
                    records=record_buffer,
                    shard_index=shard_idx,
                    chrom=chrom,
                    run_id=run_id,
                    storage=storage,
                )
                written_shards.append(shard_path)
                record_buffer = []
                shard_idx += 1

            if limit is not None and total_variants >= limit:
                logger.info(f"Reached record limit of {limit}")
                break

        if record_buffer:
            chrom = record_buffer[0]["chrom"]
            shard_path = write_parquet_shard(
                records=record_buffer,
                shard_index=shard_idx,
                chrom=chrom,
                run_id=run_id,
                storage=storage,
            )
            written_shards.append(shard_path)
            record_buffer = []
            shard_idx += 1

    # Record run manifest
    manifest = {
        "run_id": run_id,
        "source": source,
        "region": region,
        "total_variants": total_variants,
        "total_shards": len(written_shards),
        "shards": written_shards,
        "sample_count": len(sample_names),
        "created_at": datetime.now().isoformat(),
    }
    manifest_bytes = json.dumps(manifest, indent=2).encode("utf-8")
    storage.write_file(manifest_bytes, f"bronze/variants/manifest_{run_id}.json")
    logger.info(f"VCF to Parquet extraction complete: {total_variants} variants across {len(written_shards)} shards")

    return written_shards


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    parser = argparse.ArgumentParser(description="Extract VCF directly to Bronze Parquet")
    parser.add_argument("source", help="VCF file path or HTTPS URL")
    parser.add_argument("--region", default=None, help="Genomic region (e.g. chr21:10000000-10050000)")
    parser.add_argument("--lines-per-shard", type=int, default=5000)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--storage-base-path", default="data")

    args = parser.parse_args()
    vcf_to_parquet_task(
        source=args.source,
        region=args.region,
        lines_per_shard=args.lines_per_shard,
        limit=args.limit,
        storage_base_path=args.storage_base_path,
    )
