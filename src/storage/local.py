from pathlib import Path
from typing import BinaryIO, List

from .base import StorageHandler


class LocalStorageHandler(StorageHandler):
    """
    Storage handler for local filesystem.

    Manages file I/O on the local disk with automatic directory creation.
    """

    def __init__(self, base_path: str = "/data"):
        """
        Initialize local storage handler.

        Args:
            base_path: Root directory for all storage operations
        """
        self.base_path = Path(base_path).resolve()
        self.base_path.mkdir(parents=True, exist_ok=True)

    def _resolve_path(self, path: str) -> Path:
        """Resolve a path relative to base_path."""
        resolved = self.base_path / path
        # Ensure path is within base_path (security)
        if not str(resolved.resolve()).startswith(str(self.base_path)):
            raise ValueError(f"Path {path} escapes base directory")
        return resolved

    def write_file(self, data: bytes, destination: str) -> str:
        """
        Write binary data to local filesystem.

        Args:
            data: Binary data to write
            destination: Target path relative to base_path

        Returns:
            Absolute path to the written file
        """
        dest_path = self._resolve_path(destination)
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        dest_path.write_bytes(data)
        return str(dest_path)

    def read_file(self, source: str) -> bytes:
        """
        Read binary data from local filesystem.

        Args:
            source: Path relative to base_path

        Returns:
            Binary contents of the file
        """
        source_path = self._resolve_path(source)
        if not source_path.exists():
            raise FileNotFoundError(f"File not found: {source_path}")
        return source_path.read_bytes()

    def open_for_writing(self, destination: str) -> BinaryIO:
        """
        Open a file handle for writing.

        Args:
            destination: Target path relative to base_path

        Returns:
            File handle open for binary writing
        """
        dest_path = self._resolve_path(destination)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        return open(dest_path, 'wb')

    def exists(self, path: str) -> bool:
        """
        Check if a file exists.

        Args:
            path: Path relative to base_path

        Returns:
            True if file exists, False otherwise
        """
        return self._resolve_path(path).exists()

    def list_files(self, prefix: str = "") -> List[str]:
        """
        List files under a given directory.

        Args:
            prefix: Directory path relative to base_path

        Returns:
            List of file paths relative to base_path
        """
        prefix_path = self._resolve_path(prefix)
        if not prefix_path.exists():
            return []

        if prefix_path.is_file():
            return [str(prefix_path.relative_to(self.base_path))]

        files = []
        for item in prefix_path.rglob('*'):
            if item.is_file():
                files.append(str(item.relative_to(self.base_path)))
        return sorted(files)

    def delete_file(self, path: str) -> None:
        """
        Delete a file from local filesystem.

        Args:
            path: Path relative to base_path
        """
        file_path = self._resolve_path(path)
        if file_path.exists():
            file_path.unlink()

    def get_uri(self, path: str) -> str:
        """
        Get the full URI for a file path.

        Args:
            path: Relative path

        Returns:
            Full file:// URI
        """
        resolved = self._resolve_path(path)
        return f"file://{resolved}"

