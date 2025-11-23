"""Factory for creating appropriate storage handler based on configuration."""

import logging
from typing import Optional

from .base import StorageHandler
from .gcs import GCSStorageHandler
from .local import LocalStorageHandler

logger = logging.getLogger(__name__)


def create_storage(
    storage_type: str = "local",
    base_path: Optional[str] = None,
    bucket: Optional[str] = None,
    prefix: Optional[str] = None,
) -> StorageHandler:
    """
    Create appropriate storage handler based on configuration.

    Args:
        storage_type: Type of storage ('local' or 'gcs')
        base_path: Base directory for local storage
        bucket: GCS bucket name (for GCS storage)
        prefix: Optional prefix/folder (for GCS storage)

    Returns:
        Appropriate StorageHandler implementation

    Raises:
        ValueError: If storage type is not supported or required params are missing

    Examples:
        >>> storage = create_storage('local', base_path='/data')
        >>> storage = create_storage('gcs', bucket='my-bucket', prefix='pipeline-output')
    """
    storage_type = storage_type.lower()

    if storage_type == "local":
        base_path = base_path or "/data"
        logger.debug(f"Creating local storage handler at: {base_path}")
        return LocalStorageHandler(base_path=base_path)

    elif storage_type == "gcs":
        if not bucket:
            raise ValueError("GCS storage requires 'bucket' parameter")
        logger.debug(f"Creating GCS storage handler for bucket: {bucket}")
        return GCSStorageHandler(bucket=bucket, prefix=prefix or "")

    else:
        logger.error(f"Unsupported storage type: {storage_type}")
        raise ValueError(
            f"Unsupported storage type: {storage_type}. "
            f"Supported types: local, gcs"
        )

