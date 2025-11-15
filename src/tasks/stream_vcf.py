"""Task for streaming VCF data."""
import logging
from typing import Optional

from src.streaming import create_streamer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def stream_vcf_task(source: str, region: Optional[str] = None, limit: Optional[int] = None):
    """
    Stream VCF variants and print basic statistics.

    This is a simple demonstration task that streams variants and outputs
    basic information to stdout. In future iterations, this will be extended
    to shard the data into multiple BCF files.

    Args:
        source: Path or URL to VCF file
        region: Optional genomic region to filter (e.g., 'chr21', 'chr21:1000000-2000000')
        limit: Optional limit on number of variants to process (for testing)
    """
    logger.info(f"Starting VCF streaming from: {source}")
    if region:
        logger.info(f"Filtering to region: {region}")
    if limit:
        logger.info(f"Processing limit set to {limit} variants")

    try:
        # Create appropriate streamer based on source
        streamer = create_streamer(source, region)
        logger.info(f"Created streamer: {streamer.__class__.__name__}")

        # Stream variants and collect statistics
        variant_count = 0
        chrom_counts = {}

        with streamer:
            for variant in streamer.stream():
                variant_count += 1

                # Track per-chromosome counts
                chrom_counts[variant.chrom] = chrom_counts.get(variant.chrom, 0) + 1

                # Log progress periodically
                if variant_count % 10000 == 0:
                    logger.info(f"Processed {variant_count:,} variants...")

                # Log first few variants for visibility
                if variant_count <= 5:
                    logger.debug(f"Variant {variant_count}: {variant.chrom}:{variant.pos} "
                                f"{variant.ref}>{','.join(variant.alts)} "
                                f"(QUAL={variant.qual})")

                # Respect limit if provided
                if limit and variant_count >= limit:
                    logger.info(f"Reached configured limit of {limit} variants")
                    break

        # Log summary statistics
        logger.info("="*60)
        logger.info("Streaming complete!")
        logger.info(f"Total variants processed: {variant_count:,}")
        logger.info("Per-chromosome breakdown:")
        for chrom in sorted(chrom_counts.keys()):
            logger.info(f"  {chrom}: {chrom_counts[chrom]:,} variants")
        logger.info("="*60)

        return {
            "total_variants": variant_count,
            "chromosomes": chrom_counts,
        }

    except Exception as e:
        logger.error(f"Failed to stream VCF: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    """Allow running task directly for testing."""
    import argparse

    parser = argparse.ArgumentParser(description="Stream and analyze VCF file")
    parser.add_argument("source", help="Path or URL to VCF file")
    parser.add_argument("--region", help="Genomic region to filter")
    parser.add_argument("--limit", type=int, help="Max variants to process")

    args = parser.parse_args()

    stream_vcf_task(args.source, args.region, args.limit)

