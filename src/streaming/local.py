import os
from typing import Iterator, Optional

import pysam

from .base import VCFStreamer


class LocalVCFStreamer(VCFStreamer):
    """
    Stream VCF data from local filesystem using pysam.
    """

    def __init__(self, source: str, region: Optional[str] = None):
        if not os.path.exists(source):
            raise FileNotFoundError(f"VCF file not found: {source}")
        super().__init__(source, region)

    def open(self) -> None:
        if self._vcf_handle is not None:
            return
        self._vcf_handle = pysam.VariantFile(self.source)

    def close(self) -> None:
        if self._vcf_handle is not None:
            self._vcf_handle.close()
            self._vcf_handle = None

    @VCFStreamer.requires_open
    def get_header(self) -> pysam.VariantHeader:
        return self._vcf_handle.header

    @VCFStreamer.requires_open
    def stream(self) -> Iterator[pysam.VariantRecord]:
        iterator = (
            self._vcf_handle.fetch(region=self.region)
            if self.region else self._vcf_handle
        )
        for record in iterator:
            yield record
