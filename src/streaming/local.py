"""Local file VCF streaming implementation."""
import logging
import os
from typing import Iterator, Optional

import pysam

from src.models import VariantRecord

from .base import VCFStreamer

logger = logging.getLogger(__name__)


class LocalVCFStreamer(VCFStreamer):
    """
    Stream VCF data from local filesystem.

    Supports both compressed (.vcf.gz, .bcf) and uncompressed (.vcf) files.
    Uses pysam/htslib for efficient parsing and optional region filtering.
    """

    def __init__(self, source: str, region: Optional[str] = None):
        """
        Initialize local file streamer.

        Args:
            source: Path to local VCF file
            region: Optional genomic region filter

        Raises:
            FileNotFoundError: If file doesn't exist
        """
        super().__init__(source, region)

        if not os.path.exists(source):
            logger.error(f"VCF file not found: {source}")
            raise FileNotFoundError(f"VCF file not found: {source}")

        file_size = os.path.getsize(source)
        logger.info(f"Opening local VCF file: {source} ({file_size / (1024**2):.2f} MB)")

        self._vcf_handle = None

    def stream(self) -> Iterator[VariantRecord]:
        """
        Stream variants from local VCF file.

        Yields:
            VariantRecord objects
        """
        try:
            # Open VCF file with pysam
            self._vcf_handle = pysam.VariantFile(self.source)

            # Log sample count if available (defensive for testing/mocking)
            try:
                num_samples = len(self._vcf_handle.header.samples)
                logger.debug(f"Opened VCF file with {num_samples} samples")
            except (TypeError, AttributeError):
                logger.debug("Opened VCF file")

            # Fetch from region if specified, otherwise iterate all
            if self.region:
                logger.info(f"Fetching variants from region: {self.region}")
                iterator = self._vcf_handle.fetch(region=self.region)
            else:
                iterator = self._vcf_handle

            # Convert pysam records to our VariantRecord model
            for record in iterator:
                yield self._convert_record(record)

        except Exception as e:
            logger.error(f"Error while streaming VCF: {e}")
            raise
        finally:
            if self._vcf_handle:
                self._vcf_handle.close()
                logger.debug("Closed VCF file handle")
                self._vcf_handle = None

    def _convert_record(self, pysam_record) -> VariantRecord:
        """
        Convert pysam VariantRecord to our VariantRecord model.

        Args:
            pysam_record: pysam.VariantRecord object

        Returns:
            Our VariantRecord dataclass
        """
        return VariantRecord(
            chrom=pysam_record.chrom,
            pos=pysam_record.pos,
            ref=pysam_record.ref,
            alts=tuple(pysam_record.alts) if pysam_record.alts else (),
            id=pysam_record.id if pysam_record.id else None,
            qual=float(pysam_record.qual) if pysam_record.qual is not None else None,
            filter=",".join(pysam_record.filter) if pysam_record.filter else None,
        )

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensure file handle is closed."""
        if self._vcf_handle:
            self._vcf_handle.close()
            self._vcf_handle = None
