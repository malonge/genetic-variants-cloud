"""HTTPS VCF streaming implementation."""
from typing import Iterator, Optional

import pysam

from src.models import VariantRecord

from .base import VCFStreamer


class HttpsVCFStreamer(VCFStreamer):
    """
    Stream VCF data from remote HTTPS URLs.

    Uses pysam's built-in support for streaming remote files via HTTP/HTTPS.
    This leverages htslib's ability to make range requests, enabling efficient
    streaming without downloading the entire file.

    Note: Remote files should ideally have an index (.tbi or .csi) for
    optimal performance, especially when using region filtering.
    """

    def __init__(self, source: str, region: Optional[str] = None):
        """
        Initialize HTTPS streamer.

        Args:
            source: HTTPS URL to VCF file
            region: Optional genomic region filter

        Raises:
            ValueError: If URL is not HTTPS
        """
        super().__init__(source, region)

        if not source.startswith("https://"):
            raise ValueError(f"Source must be HTTPS URL, got: {source}")

        self._vcf_handle = None

    def stream(self) -> Iterator[VariantRecord]:
        """
        Stream variants from remote HTTPS VCF file.

        Yields:
            VariantRecord objects

        Raises:
            IOError: If remote file cannot be accessed
            ValueError: If file format is invalid
        """
        try:
            self._vcf_handle = pysam.VariantFile(self.source)

            # Fetch from region if specified, otherwise iterate all
            if self.region:
                iterator = self._vcf_handle.fetch(region=self.region)
            else:
                iterator = self._vcf_handle

            # Convert pysam records to our VariantRecord model
            for record in iterator:
                yield self._convert_record(record)

        except Exception as e:
            raise IOError(f"Failed to stream from {self.source}: {e}") from e
        finally:
            if self._vcf_handle:
                self._vcf_handle.close()
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
        """Ensure connection is closed."""
        if self._vcf_handle:
            self._vcf_handle.close()
            self._vcf_handle = None

