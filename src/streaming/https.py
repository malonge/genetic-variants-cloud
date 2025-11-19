"""HTTPS VCF streaming implementation."""
from typing import Iterator, Optional
import pysam

from .base import VCFStreamer


class HttpsVCFStreamer(VCFStreamer):
    """
    Stream VCF data from remote HTTPS URLs.

    Uses pysam/htslib's native HTTP(s) support including range requests for
    indexed VCF/BCF files. Works efficiently even for region queries if the
    remote endpoint supports HTTP range requests.
    """

    def __init__(self, source: str, region: Optional[str] = None):
        if not source.startswith("https://"):
            raise ValueError(f"Source must be HTTPS URL, got: {source}")
        super().__init__(source, region)

    def open(self) -> None:
        """Open the remote VCF using pysam's HTTPS streaming support."""
        if self._vcf_handle is not None:
            return

        try:
            self._vcf_handle = pysam.VariantFile(self.source)
        except Exception as e:
            raise IOError(f"Failed to open remote VCF {self.source}: {e}")

    def close(self) -> None:
        """Close the remote handle if open."""
        if self._vcf_handle is not None:
            self._vcf_handle.close()
            self._vcf_handle = None

    @VCFStreamer.requires_open
    def get_header(self) -> pysam.VariantHeader:
        """Retrieve VCF header."""
        return self._vcf_handle.header

    @VCFStreamer.requires_open
    def stream(self) -> Iterator[pysam.VariantRecord]:
        """
        Stream variants from the remote VCF file.

        Yields:
            pysam.VariantRecord objects
        """
        try:
            # Region streaming or full streaming
            iterator = (
                self._vcf_handle.fetch(region=self.region)
                if self.region else self._vcf_handle
            )

            for record in iterator:
                yield record

        except Exception as e:
            raise IOError(f"Failed to stream from remote VCF {self.source}: {e}")
