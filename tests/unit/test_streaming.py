"""Tests for VCF streaming implementations."""
import os
import tempfile
from unittest.mock import Mock, patch

import pytest

from src.models import VariantRecord
from src.streaming import (
    HttpsVCFStreamer,
    LocalVCFStreamer,
    VCFStreamer,
    create_streamer,
)


class TestVCFStreamerBase:
    """Test abstract VCFStreamer base class."""

    def test_cannot_instantiate_abstract_class(self):
        """Test that abstract base class cannot be instantiated."""
        with pytest.raises(TypeError):
            VCFStreamer("test.vcf")

    def test_base_class_has_required_methods(self):
        """Test that base class defines required abstract methods."""
        assert hasattr(VCFStreamer, 'stream')
        assert hasattr(VCFStreamer, '__enter__')
        assert hasattr(VCFStreamer, '__exit__')


class TestLocalVCFStreamer:
    """Test LocalVCFStreamer implementation."""

    def test_init_with_nonexistent_file_raises_error(self):
        """Test that initializing with nonexistent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            LocalVCFStreamer("/path/to/nonexistent/file.vcf.gz")

    def test_init_with_existing_file(self, tmp_path):
        """Test initialization with an existing file."""
        vcf_file = tmp_path / "test.vcf"
        vcf_file.write_text("##fileformat=VCFv4.2\n")

        streamer = LocalVCFStreamer(str(vcf_file))
        assert streamer.source == str(vcf_file)
        assert streamer.region is None

    def test_init_with_region(self, tmp_path):
        """Test initialization with genomic region."""
        vcf_file = tmp_path / "test.vcf"
        vcf_file.write_text("##fileformat=VCFv4.2\n")

        streamer = LocalVCFStreamer(str(vcf_file), region="chr21:1000-2000")
        assert streamer.region == "chr21:1000-2000"

    @patch('src.streaming.local.pysam.VariantFile')
    def test_stream_converts_pysam_records(self, mock_variant_file):
        """Test that stream method converts pysam records to VariantRecord."""
        # Create a mock pysam record
        mock_record = Mock()
        mock_record.chrom = "chr21"
        mock_record.pos = 1000
        mock_record.ref = "A"
        mock_record.alts = ("G", "T")
        mock_record.id = "rs12345"
        mock_record.qual = 99.5
        mock_record.filter = ["PASS"]

        # Setup mock variant file to return our mock record
        mock_vcf = Mock()
        mock_vcf.__iter__ = Mock(return_value=iter([mock_record]))
        mock_variant_file.return_value = mock_vcf

        # Create temp file for the test
        with tempfile.NamedTemporaryFile(suffix=".vcf", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # Stream and check results
            streamer = LocalVCFStreamer(tmp_path)
            variants = list(streamer.stream())

            assert len(variants) == 1
            variant = variants[0]

            assert isinstance(variant, VariantRecord)
            assert variant.chrom == "chr21"
            assert variant.pos == 1000
            assert variant.ref == "A"
            assert variant.alts == ("G", "T")
            assert variant.id == "rs12345"
            assert variant.qual == 99.5
            assert variant.filter == "PASS"
        finally:
            os.unlink(tmp_path)

    @patch('src.streaming.local.pysam.VariantFile')
    def test_stream_with_region(self, mock_variant_file):
        """Test streaming with region filter."""
        mock_record = Mock()
        mock_record.chrom = "chr21"
        mock_record.pos = 1500
        mock_record.ref = "A"
        mock_record.alts = ("G",)
        mock_record.id = None
        mock_record.qual = None
        mock_record.filter = []

        mock_vcf = Mock()
        mock_vcf.fetch = Mock(return_value=iter([mock_record]))
        mock_variant_file.return_value = mock_vcf

        with tempfile.NamedTemporaryFile(suffix=".vcf", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            streamer = LocalVCFStreamer(tmp_path, region="chr21:1000-2000")
            variants = list(streamer.stream())

            # Verify fetch was called with region
            mock_vcf.fetch.assert_called_once_with(region="chr21:1000-2000")
            assert len(variants) == 1
        finally:
            os.unlink(tmp_path)

    @patch('src.streaming.local.pysam.VariantFile')
    def test_stream_handles_no_alts(self, mock_variant_file):
        """Test handling variants with no alternate alleles."""
        mock_record = Mock()
        mock_record.chrom = "chr21"
        mock_record.pos = 1000
        mock_record.ref = "A"
        mock_record.alts = None  # No alts
        mock_record.id = None
        mock_record.qual = None
        mock_record.filter = []

        mock_vcf = Mock()
        mock_vcf.__iter__ = Mock(return_value=iter([mock_record]))
        mock_variant_file.return_value = mock_vcf

        with tempfile.NamedTemporaryFile(suffix=".vcf", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            streamer = LocalVCFStreamer(tmp_path)
            variants = list(streamer.stream())

            assert len(variants) == 1
            assert variants[0].alts == tuple()
        finally:
            os.unlink(tmp_path)

    @patch('src.streaming.local.pysam.VariantFile')
    def test_context_manager_closes_file(self, mock_variant_file):
        """Test that context manager properly closes file."""
        mock_vcf = Mock()
        mock_vcf.__iter__ = Mock(return_value=iter([]))
        mock_variant_file.return_value = mock_vcf

        with tempfile.NamedTemporaryFile(suffix=".vcf", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            with LocalVCFStreamer(tmp_path) as streamer:
                list(streamer.stream())

            # Verify close was called
            mock_vcf.close.assert_called()
        finally:
            os.unlink(tmp_path)


class TestHttpsVCFStreamer:
    """Test HttpsVCFStreamer implementation."""

    def test_init_with_non_https_url_raises_error(self):
        """Test that non-HTTPS URLs raise ValueError."""
        with pytest.raises(ValueError, match="must be HTTPS"):
            HttpsVCFStreamer("http://example.com/file.vcf.gz")

        with pytest.raises(ValueError, match="must be HTTPS"):
            HttpsVCFStreamer("ftp://example.com/file.vcf.gz")

    def test_init_with_https_url(self):
        """Test initialization with valid HTTPS URL."""
        streamer = HttpsVCFStreamer("https://example.com/file.vcf.gz")
        assert streamer.source == "https://example.com/file.vcf.gz"
        assert streamer.region is None

    def test_init_with_region(self):
        """Test initialization with genomic region."""
        streamer = HttpsVCFStreamer(
            "https://example.com/file.vcf.gz",
            region="chr21"
        )
        assert streamer.region == "chr21"

    @patch('src.streaming.https.pysam.VariantFile')
    def test_stream_converts_pysam_records(self, mock_variant_file):
        """Test that stream method converts pysam records."""
        mock_record = Mock()
        mock_record.chrom = "chr21"
        mock_record.pos = 1000
        mock_record.ref = "A"
        mock_record.alts = ("G",)
        mock_record.id = "rs12345"
        mock_record.qual = 99.5
        mock_record.filter = ["PASS"]

        mock_vcf = Mock()
        mock_vcf.__iter__ = Mock(return_value=iter([mock_record]))
        mock_variant_file.return_value = mock_vcf

        streamer = HttpsVCFStreamer("https://example.com/file.vcf.gz")
        variants = list(streamer.stream())

        assert len(variants) == 1
        variant = variants[0]

        assert isinstance(variant, VariantRecord)
        assert variant.chrom == "chr21"
        assert variant.pos == 1000

    @patch('src.streaming.https.pysam.VariantFile')
    def test_stream_with_region(self, mock_variant_file):
        """Test streaming with region filter."""
        mock_record = Mock()
        mock_record.chrom = "chr21"
        mock_record.pos = 1500
        mock_record.ref = "A"
        mock_record.alts = ("G",)
        mock_record.id = None
        mock_record.qual = None
        mock_record.filter = []

        mock_vcf = Mock()
        mock_vcf.fetch = Mock(return_value=iter([mock_record]))
        mock_variant_file.return_value = mock_vcf

        streamer = HttpsVCFStreamer(
            "https://example.com/file.vcf.gz",
            region="chr21:1000-2000"
        )
        variants = list(streamer.stream())

        mock_vcf.fetch.assert_called_once_with(region="chr21:1000-2000")
        assert len(variants) == 1

    @patch('src.streaming.https.pysam.VariantFile')
    def test_stream_raises_ioerror_on_failure(self, mock_variant_file):
        """Test that streaming failures raise IOError."""
        mock_variant_file.side_effect = Exception("Connection failed")

        streamer = HttpsVCFStreamer("https://example.com/file.vcf.gz")

        with pytest.raises(IOError, match="Failed to stream"):
            list(streamer.stream())

    @patch('src.streaming.https.pysam.VariantFile')
    def test_context_manager_closes_connection(self, mock_variant_file):
        """Test that context manager properly closes connection."""
        mock_vcf = Mock()
        mock_vcf.__iter__ = Mock(return_value=iter([]))
        mock_variant_file.return_value = mock_vcf

        with HttpsVCFStreamer("https://example.com/file.vcf.gz") as streamer:
            list(streamer.stream())

        mock_vcf.close.assert_called()


class TestStreamerFactory:
    """Test create_streamer factory function."""

    def test_create_local_streamer_from_path(self, tmp_path):
        """Test creating local streamer from file path."""
        vcf_file = tmp_path / "test.vcf"
        vcf_file.write_text("##fileformat=VCFv4.2\n")

        streamer = create_streamer(str(vcf_file))
        assert isinstance(streamer, LocalVCFStreamer)
        assert streamer.source == str(vcf_file)

    def test_create_local_streamer_from_file_uri(self, tmp_path):
        """Test creating local streamer from file:// URI."""
        vcf_file = tmp_path / "test.vcf"
        vcf_file.write_text("##fileformat=VCFv4.2\n")

        streamer = create_streamer(f"file://{vcf_file}")
        assert isinstance(streamer, LocalVCFStreamer)

    def test_create_https_streamer(self):
        """Test creating HTTPS streamer from URL."""
        streamer = create_streamer("https://example.com/file.vcf.gz")
        assert isinstance(streamer, HttpsVCFStreamer)
        assert streamer.source == "https://example.com/file.vcf.gz"

    def test_create_streamer_with_region(self, tmp_path):
        """Test creating streamer with region parameter."""
        vcf_file = tmp_path / "test.vcf"
        vcf_file.write_text("##fileformat=VCFv4.2\n")

        streamer = create_streamer(str(vcf_file), region="chr21")
        assert streamer.region == "chr21"

    def test_unsupported_scheme_raises_error(self):
        """Test that unsupported URI schemes raise ValueError."""
        with pytest.raises(ValueError, match="Unsupported source scheme"):
            create_streamer("ftp://example.com/file.vcf.gz")

        with pytest.raises(ValueError, match="Unsupported source scheme"):
            create_streamer("s3://bucket/file.vcf.gz")

