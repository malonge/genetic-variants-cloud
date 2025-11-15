"""Factory for creating appropriate VCF streamer based on source."""

import logging
from typing import Optional
from urllib.parse import urlparse

from .base import VCFStreamer
from .https import HttpsVCFStreamer
from .local import LocalVCFStreamer

logger = logging.getLogger(__name__)


def create_streamer(source: str, region: Optional[str] = None) -> VCFStreamer:
    """
    Create appropriate VCF streamer based on source URI scheme.

    Args:
        source: URI to VCF file (local path or URL)
        region: Optional genomic region filter

    Returns:
        Appropriate VCFStreamer implementation

    Raises:
        ValueError: If source scheme is not supported

    Examples:
        >>> streamer = create_streamer("/path/to/file.vcf.gz")
        >>> streamer = create_streamer("https://example.com/variants.vcf.gz", region="chr21")
    """
    parsed = urlparse(source)

    # HTTPS URL
    if parsed.scheme == "https":
        logger.debug(f"Creating HTTPS streamer for: {source}")
        return HttpsVCFStreamer(source, region)

    # Local file (no scheme or file:// scheme)
    if parsed.scheme in ("", "file"):
        # Remove file:// prefix if present
        local_path = parsed.path if parsed.scheme == "file" else source
        logger.debug(f"Creating local file streamer for: {local_path}")
        return LocalVCFStreamer(local_path, region)

    # Unsupported scheme
    logger.error(f"Unsupported source scheme: {parsed.scheme}")
    raise ValueError(
        f"Unsupported source scheme: {parsed.scheme}. "
        f"Supported schemes: file://, https://, or local paths"
    )
