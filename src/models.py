"""Core data models for variant records."""

from typing import Optional

from pydantic import BaseModel, Field, field_validator


class VariantRecord(BaseModel):
    """
    Lightweight representation of a genetic variant.

    This abstraction decouples the pipeline from specific VCF parsing libraries
    and provides a clean interface for downstream processing.

    Attributes:
        chrom: Chromosome/contig name (e.g., 'chr21', 'chrM')
        pos: 1-based genomic position
        ref: Reference allele sequence
        alts: Tuple of alternate allele sequences (empty tuple if no alts)
        id: Variant ID (e.g., rsID), None if not present
        qual: Variant quality score, None if not available
        filter: Filter status (e.g., 'PASS', 'LowQual'), None if not present
    """
    chrom: str = Field(..., min_length=1, description="Chromosome/contig name")
    pos: int = Field(..., gt=0, description="1-based genomic position")
    ref: str = Field(..., min_length=1, description="Reference allele sequence")
    alts: tuple[str, ...] = Field(default=(), description="Alternate alleles")
    id: Optional[str] = Field(default=None, description="Variant ID")
    qual: Optional[float] = Field(default=None, description="Variant quality score")
    filter: Optional[str] = Field(default=None, description="Filter status")

    model_config = {
        "frozen": True,  # Immutable
        "str_strip_whitespace": True,
    }

    @field_validator('qual')
    @classmethod
    def qual_must_be_non_negative(cls, v: Optional[float]) -> Optional[float]:
        """Validate quality score is non-negative."""
        if v is not None and v < 0:
            raise ValueError('Quality score must be non-negative')
        return v
