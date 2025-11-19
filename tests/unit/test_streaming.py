"""Tests for VCF streaming implementations."""
import os
import tempfile
from unittest.mock import Mock, patch

import pytest
import pysam

from src.streaming import (
    VCFStreamer,
    LocalVCFStreamer,
    HttpsVCFStreamer,
    create_streamer,
)


class TestVCFStreamerBase:
    """Test abstract VCFStreamer base class."""
    
    def test_cannot_instantiate_abstract_class(self):
        """Test that abstract base class cannot be instantiated."""
        with pytest.raises(TypeError):
            VCFStreamer("test.vcf")


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
    def test_stream_yields_pysam_records(self, mock_variant_file):
        """Test that stream method yields pysam.VariantRecord objects."""
        # Create a mock pysam record
        mock_record = Mock(spec=pysam.VariantRecord)
        mock_record.chrom = "chr21"
        mock_record.pos = 1000
        mock_record.ref = "A"
        mock_record.alts = ("G", "T")
        
        # Setup mock variant file
        mock_vcf = Mock()
        mock_vcf.__iter__ = Mock(return_value=iter([mock_record]))
        mock_vcf.header = Mock(spec=pysam.VariantHeader)
        mock_variant_file.return_value = mock_vcf
        
        # Create temp file for the test
        with tempfile.NamedTemporaryFile(suffix=".vcf", delete=False) as tmp:
            tmp_path = tmp.name
        
        try:
            streamer = LocalVCFStreamer(tmp_path)
            records = list(streamer.stream())
            
            assert len(records) == 1
            assert records[0].chrom == "chr21"
            assert records[0].pos == 1000
        finally:
            os.unlink(tmp_path)
    
    @patch('src.streaming.local.pysam.VariantFile')
    def test_get_header(self, mock_variant_file):
        """Test getting VCF header."""
        mock_header = Mock(spec=pysam.VariantHeader)
        mock_vcf = Mock()
        mock_vcf.header = mock_header
        mock_variant_file.return_value = mock_vcf
        
        with tempfile.NamedTemporaryFile(suffix=".vcf", delete=False) as tmp:
            tmp_path = tmp.name
        
        try:
            streamer = LocalVCFStreamer(tmp_path)
            header = streamer.get_header()
            assert header == mock_header
        finally:
            os.unlink(tmp_path)
    
    @patch('src.streaming.local.pysam.VariantFile')
    def test_stream_with_region(self, mock_variant_file):
        """Test streaming with region filter."""
        mock_record = Mock(spec=pysam.VariantRecord)
        mock_record.chrom = "chr21"
        mock_record.pos = 1500
        
        mock_vcf = Mock()
        mock_vcf.fetch = Mock(return_value=iter([mock_record]))
        mock_vcf.header = Mock(spec=pysam.VariantHeader)
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
    def test_context_manager_closes_file(self, mock_variant_file):
        """Test that context manager properly closes file."""
        mock_vcf = Mock()
        mock_vcf.__iter__ = Mock(return_value=iter([]))
        mock_vcf.header = Mock(spec=pysam.VariantHeader)
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
    
    @patch('src.streaming.https.pysam.VariantFile')
    def test_stream_yields_pysam_records(self, mock_variant_file):
        """Test that stream method yields pysam.VariantRecord objects."""
        mock_record = Mock(spec=pysam.VariantRecord)
        mock_record.chrom = "chr21"
        mock_record.pos = 1000
        
        mock_vcf = Mock()
        mock_vcf.__iter__ = Mock(return_value=iter([mock_record]))
        mock_vcf.header = Mock(spec=pysam.VariantHeader)
        mock_variant_file.return_value = mock_vcf
        
        streamer = HttpsVCFStreamer("https://example.com/file.vcf.gz")
        records = list(streamer.stream())
        
        assert len(records) == 1
        assert records[0].chrom == "chr21"
    
    @patch('src.streaming.https.pysam.VariantFile')
    def test_get_header(self, mock_variant_file):
        """Test getting VCF header from remote file."""
        mock_header = Mock(spec=pysam.VariantHeader)
        mock_vcf = Mock()
        mock_vcf.header = mock_header
        mock_variant_file.return_value = mock_vcf
        
        streamer = HttpsVCFStreamer("https://example.com/file.vcf.gz")
        header = streamer.get_header()
        assert header == mock_header
    
    @patch('src.streaming.https.pysam.VariantFile')
    def test_stream_raises_ioerror_on_failure(self, mock_variant_file):
        """Test that streaming failures raise IOError."""
        mock_variant_file.side_effect = Exception("Connection failed")
        
        streamer = HttpsVCFStreamer("https://example.com/file.vcf.gz")
        
        with pytest.raises(IOError):
            list(streamer.stream())


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
