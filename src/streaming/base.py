from abc import ABC, abstractmethod
import functools
from typing import Iterator, Optional

import pysam


class VCFStreamer(ABC):
    """
    Abstract base class for streaming VCF data.
    """

    def __init__(self, source: str, region: Optional[str] = None):
        self.source = source
        self.region = region
        self._vcf_handle = None

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _ensure_open(self):
        if self._vcf_handle is None:
            self.open()

    @staticmethod
    def requires_open(method):
        @functools.wraps(method)
        def wrapper(self, *args, **kwargs):
            self._ensure_open()
            return method(self, *args, **kwargs)
        return wrapper

    @abstractmethod
    def open(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def stream(self) -> Iterator[pysam.VariantRecord]:
        raise NotImplementedError

    @abstractmethod
    def get_header(self) -> pysam.VariantHeader:
        raise NotImplementedError
