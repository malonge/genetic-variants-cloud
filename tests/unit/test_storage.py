"""Unit tests for storage abstraction."""
from pathlib import Path

import pytest

from src.storage import create_storage
from src.storage.local import LocalStorageHandler


class TestStorageFactory:
    """Test storage factory functionality."""

    def test_create_local_storage(self, tmp_path):
        """Test creating local storage handler."""
        storage = create_storage("local", base_path=str(tmp_path))
        assert isinstance(storage, LocalStorageHandler)
        assert storage.base_path == tmp_path

    def test_create_storage_default_type(self, tmp_path):
        """Test default storage type is local."""
        storage = create_storage(base_path=str(tmp_path))
        assert isinstance(storage, LocalStorageHandler)

    def test_create_gcs_storage_not_implemented(self):
        """Test GCS storage raises NotImplementedError."""
        with pytest.raises(NotImplementedError):
            create_storage("gcs", bucket="test-bucket")

    def test_unsupported_storage_type(self):
        """Test unsupported storage type raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported storage type"):
            create_storage("s3")


class TestLocalStorage:
    """Test local storage handler functionality."""

    def test_write_and_read_file(self, tmp_path):
        """Test writing and reading files."""
        storage = LocalStorageHandler(base_path=str(tmp_path))

        test_data = b"test content"
        written_path = storage.write_file(test_data, "test_file.txt")

        assert Path(written_path).exists()
        read_data = storage.read_file("test_file.txt")
        assert read_data == test_data

    def test_write_creates_directories(self, tmp_path):
        """Test that write_file creates necessary directories."""
        storage = LocalStorageHandler(base_path=str(tmp_path))

        test_data = b"nested content"
        storage.write_file(test_data, "nested/dir/file.txt")

        assert (tmp_path / "nested" / "dir" / "file.txt").exists()

    def test_file_exists(self, tmp_path):
        """Test exists method."""
        storage = LocalStorageHandler(base_path=str(tmp_path))

        assert not storage.exists("nonexistent.txt")

        storage.write_file(b"data", "existing.txt")
        assert storage.exists("existing.txt")

    def test_list_files(self, tmp_path):
        """Test listing files."""
        storage = LocalStorageHandler(base_path=str(tmp_path))

        storage.write_file(b"1", "file1.txt")
        storage.write_file(b"2", "dir/file2.txt")
        storage.write_file(b"3", "dir/subdir/file3.txt")

        all_files = storage.list_files("")
        assert len(all_files) == 3
        assert "file1.txt" in all_files
        assert "dir/file2.txt" in all_files

        dir_files = storage.list_files("dir")
        assert len(dir_files) == 2

    def test_delete_file(self, tmp_path):
        """Test deleting files."""
        storage = LocalStorageHandler(base_path=str(tmp_path))

        storage.write_file(b"data", "to_delete.txt")
        assert storage.exists("to_delete.txt")

        storage.delete_file("to_delete.txt")
        assert not storage.exists("to_delete.txt")

    def test_get_uri(self, tmp_path):
        """Test getting file URI."""
        storage = LocalStorageHandler(base_path=str(tmp_path))

        uri = storage.get_uri("test.txt")
        assert uri.startswith("file://")
        assert uri.endswith("test.txt")

    def test_open_for_writing(self, tmp_path):
        """Test opening file handle for writing."""
        storage = LocalStorageHandler(base_path=str(tmp_path))

        with storage.open_for_writing("streamed.txt") as f:
            f.write(b"line 1\n")
            f.write(b"line 2\n")

        content = storage.read_file("streamed.txt")
        assert content == b"line 1\nline 2\n"

    def test_security_path_escape(self, tmp_path):
        """Test that paths cannot escape base directory."""
        storage = LocalStorageHandler(base_path=str(tmp_path))

        with pytest.raises(ValueError, match="escapes base directory"):
            storage.write_file(b"bad", "../outside.txt")

