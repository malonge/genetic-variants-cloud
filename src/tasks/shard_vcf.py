"""Task for sharding VCF data into BCF files."""
import logging
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import pysam

from src.storage import create_storage
from src.streaming import create_streamer

logger = logging.getLogger(__name__)


def shard_vcf_task(
    source: str,
    lines_per_shard: int = 10000,
    region: Optional[str] = None,
    storage_type: str = "local",
    storage_base_path: str = "/data",
    run_id: Optional[str] = None,
) -> List[str]:
    """
    Stream VCF data and write it as sharded, compressed, and indexed BCF files.

    Each shard contains:
    - The full original VCF header
    - Up to `lines_per_shard` variant records
    - CSI index for random access

    Shards are written to timestamped run directories for idempotency.

    Args:
        source: Path or URL to source VCF file
        lines_per_shard: Maximum number of variant records per shard
        region: Optional genomic region to filter
        storage_type: Type of storage backend ('local' or 'gcs')
        storage_base_path: Base path for local storage
        run_id: Optional run identifier (defaults to timestamp)

    Returns:
        List of shard file paths (relative to storage base)

    Example:
        >>> shards = shard_vcf_task(
        ...     source="https://example.com/variants.vcf.gz",
        ...     lines_per_shard=5000,
        ...     region="chr21"
        ... )
        >>> print(shards)
        ['2025-11-23_10-30-00/shards/shard_0000.bcf',
         '2025-11-23_10-30-00/shards/shard_0001.bcf']
    """
    # Generate run ID if not provided (timestamp-based)
    if run_id is None:
        run_id = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    logger.info("Starting VCF sharding task")
    logger.info(f"  Source: {source}")
    logger.info(f"  Lines per shard: {lines_per_shard}")
    logger.info(f"  Region: {region or 'all'}")
    logger.info(f"  Run ID: {run_id}")
    logger.info(f"  Storage: {storage_type}")

    # Initialize storage handler and streamer
    storage = create_storage(storage_type=storage_type, base_path=storage_base_path)
    streamer = create_streamer(source, region)

    try:
        # Get header
        header = streamer.get_header()
        logger.info(f"VCF version: {header.version}")
        logger.info(f"Samples: {len(header.samples)}")
        logger.info(f"Contigs: {len(header.contigs)}")

        shard_paths = []
        shard_index = 0
        variant_count = 0
        buffer = []

        # Stream the body of the VCF file
        with streamer:
            for record in streamer.stream():
                buffer.append(record)
                variant_count += 1

                # Write shard when buffer reaches limit
                if len(buffer) >= lines_per_shard:
                    shard_path = _write_shard(
                        header=header,
                        records=buffer,
                        shard_index=shard_index,
                        run_id=run_id,
                        storage=storage,
                    )
                    shard_paths.append(shard_path)
                    logger.info(f"Wrote shard {shard_index}: {len(buffer)} variants")

                    # Reset buffer and increment index
                    buffer = []
                    shard_index += 1

            # Write final shard if buffer has remaining records
            if buffer:
                shard_path = _write_shard(
                    header=header,
                    records=buffer,
                    shard_index=shard_index,
                    run_id=run_id,
                    storage=storage,
                )
                shard_paths.append(shard_path)
                logger.info(f"Wrote shard {shard_index}: {len(buffer)} variants")

        # Log summary
        logger.info("=" * 60)
        logger.info("Sharding complete!")
        logger.info(f"  Total variants: {variant_count:,}")
        logger.info(f"  Total shards: {len(shard_paths)}")
        logger.info(f"  Output directory: {run_id}/shards/")
        logger.info("=" * 60)

        return shard_paths

    except Exception as e:
        logger.error(f"Failed to shard VCF: {e}", exc_info=True)
        raise


def _write_shard(
    header: pysam.VariantHeader,
    records: List[pysam.VariantRecord],
    shard_index: int,
    run_id: str,
    storage,
) -> str:
    """
    Write a single shard as a compressed and indexed BCF file.

    Args:
        header: VCF header to include in shard
        records: Variant records to write
        shard_index: Zero-padded shard number
        run_id: Run identifier for output directory
        storage: StorageHandler instance

    Returns:
        Relative path to the written BCF file
    """
    # Format shard filename with zero-padding (up to 9999 shards)
    shard_filename = f"shard_{shard_index:04d}.bcf"
    shard_path = f"{run_id}/shards/{shard_filename}"

    # Use temporary file for writing since pysam and bcftools need real file paths
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_bcf = Path(tmpdir) / shard_filename

        # First write as VCF to avoid header validation issues with BCF
        tmp_vcf = tmp_bcf.with_suffix('.vcf.gz')
        with pysam.VariantFile(str(tmp_vcf), 'wz', header=header) as vcf_out:
            for record in records:
                vcf_out.write(record)

        # Convert VCF to BCF using bcftools (ensures proper BCF formatting)
        try:
            subprocess.run(
                ['bcftools', 'view', '-O', 'b', '-o', str(tmp_bcf), str(tmp_vcf)],
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to convert VCF to BCF: {e.stderr}")
            raise RuntimeError(f"BCF conversion failed: {e.stderr}") from e

        # Index the BCF file using bcftools (CSI index)
        # CSI index supports larger chromosomes than tabix
        try:
            subprocess.run(
                ['bcftools', 'index', '--csi', str(tmp_bcf)],
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to index BCF: {e.stderr}")
            raise RuntimeError(f"BCF indexing failed: {e.stderr}") from e

        # Read BCF and index files
        bcf_data = tmp_bcf.read_bytes()
        csi_data = (tmp_bcf.parent / f"{shard_filename}.csi").read_bytes()

        # Write to storage
        storage.write_file(bcf_data, shard_path)
        storage.write_file(csi_data, f"{shard_path}.csi")

    return shard_path


if __name__ == "__main__":
    """Allow running task directly for testing."""
    import argparse

    # Configure logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    parser = argparse.ArgumentParser(description="Shard VCF file into BCF shards")
    parser.add_argument("source", help="Path or URL to VCF file")
    parser.add_argument(
        "--lines-per-shard",
        type=int,
        default=10000,
        help="Number of variant records per shard (default: 10000)"
    )
    parser.add_argument("--region", help="Genomic region to filter")
    parser.add_argument(
        "--storage-type",
        default="local",
        choices=["local", "gcs"],
        help="Storage backend type"
    )
    parser.add_argument(
        "--storage-base-path",
        default="/data",
        help="Base path for local storage"
    )
    parser.add_argument("--run-id", help="Run identifier (defaults to timestamp)")

    args = parser.parse_args()

    shards = shard_vcf_task(
        source=args.source,
        lines_per_shard=args.lines_per_shard,
        region=args.region,
        storage_type=args.storage_type,
        storage_base_path=args.storage_base_path,
        run_id=args.run_id,
    )

    print("\nGenerated shards:")
    for shard in shards:
        print(f"  - {shard}")

