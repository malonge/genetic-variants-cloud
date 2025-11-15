"""Tests for core data models."""
from typing import Any


import pytest
from pydantic import ValidationError

from src.models import VariantRecord


class TestVariantRecord:
    """Test VariantRecord Pydantic model."""

    def test_basic_variant_creation(self):
        """Test creating a basic variant record."""
        variant = VariantRecord(
            chrom="chr21",
            pos=1000,
            ref="A",
            alts=("G",),
        )

        assert variant.chrom == "chr21"
        assert variant.pos == 1000
        assert variant.ref == "A"
        assert variant.alts == ("G",)
        assert variant.id is None
        assert variant.qual is None
        assert variant.filter is None

    def test_variant_with_all_fields(self):
        """Test creating variant with all optional fields."""
        variant = VariantRecord(
            chrom="chr21",
            pos=1000,
            ref="A",
            alts=("G", "T"),
            id="rs12345",
            qual=99.5,
            filter="PASS",
        )

        assert variant.id == "rs12345"
        assert variant.qual == 99.5
        assert variant.filter == "PASS"
        assert len(variant.alts) == 2

    def test_variant_with_no_alts(self):
        """Test variant with no alternate alleles."""
        variant = VariantRecord(
            chrom="chr21",
            pos=1000,
            ref="A",
            alts=tuple[Any, ...](),
        )

        assert variant.alts == tuple[Any, ...]()
        assert len(variant.alts) == 0

    def test_variant_is_immutable(self):
        """Test that variant records are frozen (immutable)."""
        variant = VariantRecord(
            chrom="chr21",
            pos=1000,
            ref="A",
            alts=("G",),
        )

        with pytest.raises(ValidationError):
            variant.pos = 2000

    def test_invalid_position_raises_error(self):
        """Test that invalid positions raise ValidationError."""
        with pytest.raises(ValidationError):
            VariantRecord(
                chrom="chr21",
                pos=0,
                ref="A",
                alts=("G",),
            )

    def test_empty_chromosome_raises_error(self):
        """Test that empty chromosome raises ValidationError."""
        with pytest.raises(ValidationError):
            VariantRecord(
                chrom="",
                pos=1000,
                ref="A",
                alts=("G",),
            )

    def test_empty_ref_raises_error(self):
        """Test that empty reference allele raises ValidationError."""
        with pytest.raises(ValidationError):
            VariantRecord(
                chrom="chr21",
                pos=1000,
                ref="",
                alts=("G",),
            )

    def test_negative_quality_raises_error(self):
        """Test that negative quality scores raise ValidationError."""
        with pytest.raises(ValidationError, match="Quality score must be non-negative"):
            VariantRecord(
                chrom="chr21",
                pos=1000,
                ref="A",
                alts=("G",),
                qual=-1.0,
            )

    def test_whitespace_stripped(self):
        """Test that whitespace is stripped from string fields."""
        variant = VariantRecord(
            chrom=" chr21 ",
            pos=1000,
            ref=" A ",
            alts=("G",),
        )

        assert variant.chrom == "chr21"
        assert variant.ref == "A"

    def test_variant_equality(self):
        """Test that variants with same values are equal."""
        variant1 = VariantRecord(
            chrom="chr21",
            pos=1000,
            ref="A",
            alts=("G",),
        )
        variant2 = VariantRecord(
            chrom="chr21",
            pos=1000,
            ref="A",
            alts=("G",),
        )

        assert variant1 == variant2

    def test_variant_inequality(self):
        """Test that variants with different values are not equal."""
        variant1 = VariantRecord(
            chrom="chr21",
            pos=1000,
            ref="A",
            alts=("G",),
        )
        variant2 = VariantRecord(
            chrom="chr21",
            pos=2000,
            ref="A",
            alts=("G",),
        )

        assert variant1 != variant2

