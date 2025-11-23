"""Task for streaming VCF data."""
import logging
from typing import Optional

from src.streaming import create_streamer

logger = logging.getLogger(__name__)


def stream_vcf_task(source: str, region: Optional[str] = None, limit: Optional[int] = None):
    """
    Stream VCF variants and print basic statistics.

    This task demonstrates the streaming abstraction by reading variants
    and outputting statistics. The actual variant records are pysam.VariantRecord
    objects which maintain the full VCF data for future sharding.

    Args:
        source: Path or URL to VCF file
        region: Optional genomic region to filter (e.g., 'chr21', 'chr21:1000000-2000000')
        limit: Optional limit on number of variants to process (for testing)
    """
    logger.info(f"Starting VCF streaming from: {source}")
    if region:
        logger.info(f"Filtering to region: {region}")

    try:
        # Create appropriate streamer based on source
        streamer = create_streamer(source, region)

        # Get header info
        header = streamer.get_header()
        logger.info(f"VCF version: {header.version}")
        logger.info(f"Samples: {len(header.samples)} ({', '.join(list(header.samples)[:5])}...)")
        logger.info(f"Contigs: {len(header.contigs)}")

        # Stream variants and collect statistics
        variant_count = 0
        chrom_counts = {}
        total_bytes = 0

        with streamer:
            for record in streamer.stream():
                variant_count += 1

                # Track per-chromosome counts
                chrom_counts[record.chrom] = chrom_counts.get(record.chrom, 0) + 1

                # Estimate record size (for future shard size planning)
                # This is approximate - actual serialization may differ
                record_str = str(record)
                total_bytes += len(record_str.encode('utf-8'))

                # Print first few variants for visibility
                if variant_count <= 10:
                    alts = ','.join(record.alts) if record.alts else '.'
                    print(f"Variant {variant_count}: {record.chrom}:{record.pos} "
                          f"{record.ref}>{alts} "
                          f"(QUAL={record.qual})")

                # Respect limit if provided
                if limit and variant_count >= limit:
                    logger.info(f"Reached limit of {limit} variants")
                    break

        # Log summary statistics
        logger.info("=" * 60)
        logger.info("Streaming complete!")
        logger.info(f"Total variants processed: {variant_count:,}")
        logger.info(f"Total size (approx): {total_bytes:,} bytes ({total_bytes / 1024 / 1024:.2f} MB)")
        if variant_count > 0:
            logger.info(f"Average variant size: {total_bytes / variant_count:.1f} bytes")
        logger.info("Per-chromosome breakdown:")
        for chrom in sorted(chrom_counts.keys()):
            logger.info(f"  {chrom}: {chrom_counts[chrom]:,} variants")
        logger.info("=" * 60)

        return {
            "total_variants": variant_count,
            "total_bytes": total_bytes,
            "chromosomes": chrom_counts,
        }

    except Exception as e:
        logger.error(f"Failed to stream VCF: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    """Allow running task directly for testing."""
    import argparse

    # Configure logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    parser = argparse.ArgumentParser(description="Stream and analyze VCF file")
    parser.add_argument("source", help="Path or URL to VCF file")
    parser.add_argument("--region", help="Genomic region to filter")
    parser.add_argument("--limit", type=int, help="Max variants to process")

    args = parser.parse_args()

    stream_vcf_task(args.source, args.region, args.limit)
