"""Abstract base class for VCF streaming."""

from abc import ABC, abstractmethod
from typing import Iterator, Optional

from src.models import VariantRecord


class VCFStreamer(ABC):
    """
    Abstract base class for streaming VCF data from various sources.

    This abstraction allows the pipeline to stream variants from local files,
    remote URLs, cloud storage, etc. without changing downstream logic.

    Implementations must handle:
    - Opening/closing the data source
    - Parsing VCF format
    - Converting to VariantRecord objects
    - Optional region filtering
    """

    def __init__(self, source: str, region: Optional[str] = None):
        """
        Initialize the streamer.

        Args:
            source: Source identifier (file path, URL, etc.)
            region: Optional genomic region in format 'chr:start-end' or 'chr'
        """
        self.source = source
        self.region = region

    @abstractmethod
    def stream(self) -> Iterator[VariantRecord]:
        """
        Stream variant records from the source.

        Yields:
            VariantRecord objects representing individual variants

        Raises:
            FileNotFoundError: If source cannot be found
            ValueError: If source format is invalid
            IOError: If source cannot be read
        """
        pass

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - subclasses can override for cleanup."""
        pass
