from abc import ABC, abstractmethod
from typing import BinaryIO, List


class StorageHandler(ABC):
    """
    Abstract base class for file storage operations.

    Provides a unified interface for storing pipeline artifacts
    (shards, TSV files, Parquet files) across different storage backends.
    """

    @abstractmethod
    def write_file(self, data: bytes, destination: str) -> str:
        """
        Write binary data to storage.

        Args:
            data: Binary data to write
            destination: Target path/key in storage

        Returns:
            Full path/URI to the written file
        """
        raise NotImplementedError

    @abstractmethod
    def read_file(self, source: str) -> bytes:
        """
        Read binary data from storage.

        Args:
            source: Path/key to read from

        Returns:
            Binary contents of the file
        """
        raise NotImplementedError

    @abstractmethod
    def open_for_writing(self, destination: str) -> BinaryIO:
        """
        Open a file handle for writing.

        Useful for streaming large files or when pysam/other libraries
        need a file-like object.

        Args:
            destination: Target path/key in storage

        Returns:
            File-like object open for binary writing
        """
        raise NotImplementedError

    @abstractmethod
    def exists(self, path: str) -> bool:
        """
        Check if a file exists in storage.

        Args:
            path: Path/key to check

        Returns:
            True if file exists, False otherwise
        """
        raise NotImplementedError

    @abstractmethod
    def list_files(self, prefix: str) -> List[str]:
        """
        List files under a given prefix/directory.

        Args:
            prefix: Directory path or prefix to list

        Returns:
            List of file paths/keys
        """
        raise NotImplementedError

    @abstractmethod
    def delete_file(self, path: str) -> None:
        """
        Delete a file from storage.

        Args:
            path: Path/key to delete
        """
        raise NotImplementedError

    @abstractmethod
    def get_uri(self, path: str) -> str:
        """
        Get the full URI for a file path.

        Args:
            path: Relative or absolute path

        Returns:
            Full URI (e.g., 'file:///path/to/file' or 'gs://bucket/path')
        """
        raise NotImplementedError
