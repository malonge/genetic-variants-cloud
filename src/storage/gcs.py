from typing import BinaryIO, List

from .base import StorageHandler


class GCSStorageHandler(StorageHandler):
    """
    Storage handler for Google Cloud Storage.

    This is a stub implementation that will be completed when GCS integration
    is needed. It will use the google-cloud-storage Python client library.

    Example usage (future):
        storage = GCSStorageHandler(bucket='my-bucket', prefix='pipeline-output')
        storage.write_file(data, 'run_001/shard_0000.bcf')
    """

    def __init__(self, bucket: str, prefix: str = ""):
        """
        Initialize GCS storage handler.

        Args:
            bucket: GCS bucket name
            prefix: Optional prefix/folder within bucket
        """
        self.bucket = bucket
        self.prefix = prefix.rstrip('/')
        # TODO: Initialize GCS client when implemented
        # from google.cloud import storage
        # self.client = storage.Client()
        # self.bucket_obj = self.client.bucket(bucket)
        raise NotImplementedError(
            "GCS storage handler not yet implemented. "
            "Will be added when cloud deployment is needed."
        )

    def write_file(self, data: bytes, destination: str) -> str:
        """Write to GCS (not yet implemented)."""
        raise NotImplementedError("GCS integration pending")

    def read_file(self, source: str) -> bytes:
        """Read from GCS (not yet implemented)."""
        raise NotImplementedError("GCS integration pending")

    def open_for_writing(self, destination: str) -> BinaryIO:
        """Open GCS blob for writing (not yet implemented)."""
        raise NotImplementedError("GCS integration pending")

    def exists(self, path: str) -> bool:
        """Check if blob exists in GCS (not yet implemented)."""
        raise NotImplementedError("GCS integration pending")

    def list_files(self, prefix: str) -> List[str]:
        """List blobs in GCS (not yet implemented)."""
        raise NotImplementedError("GCS integration pending")

    def delete_file(self, path: str) -> None:
        """Delete blob from GCS (not yet implemented)."""
        raise NotImplementedError("GCS integration pending")

    def get_uri(self, path: str) -> str:
        """Get GCS URI (not yet implemented)."""
        raise NotImplementedError("GCS integration pending")

