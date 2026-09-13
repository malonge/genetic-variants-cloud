"""
Data models and PyArrow schema definitions for the genomic ELT pipeline.
"""
from typing import List, Optional

import pyarrow as pa
from pydantic import BaseModel, Field


class VariantRecordModel(BaseModel):
    """Pydantic model representing a single normalized genomic variant record."""
    chrom: str
    pos: int
    id: Optional[str] = None
    ref: str
    alt: List[str]
    qual: Optional[float] = None
    filter: List[str] = Field(default_factory=list)
    info_dp: Optional[int] = None
    info_ac: List[int] = Field(default_factory=list)
    info_af: List[float] = Field(default_factory=list)
    info_an: Optional[int] = None


class SampleCallModel(BaseModel):
    """Sample-level genotype call model."""
    sample_id: str
    gt: Optional[str] = None
    gq: Optional[int] = None
    dp: Optional[int] = None


class SampleMetadataModel(BaseModel):
    """IGSR sample metadata model."""
    family_id: str
    sample_id: str
    father_id: str
    mother_id: str
    sex: int
    population: str
    superpopulation: str


# PyArrow Schema for the Bronze Parquet table
# Retains nested sample calls as an array of structs for scalable columnar storage.
SAMPLE_STRUCT_TYPE = pa.struct([
    pa.field("sample_id", pa.string(), nullable=False),
    pa.field("gt", pa.string(), nullable=True),
    pa.field("gq", pa.int32(), nullable=True),
    pa.field("dp", pa.int32(), nullable=True),
])

VARIANT_PARQUET_SCHEMA = pa.schema([
    pa.field("chrom", pa.string(), nullable=False),
    pa.field("pos", pa.int64(), nullable=False),
    pa.field("id", pa.string(), nullable=True),
    pa.field("ref", pa.string(), nullable=False),
    pa.field("alt", pa.list_(pa.string()), nullable=False),
    pa.field("qual", pa.float64(), nullable=True),
    pa.field("filter", pa.list_(pa.string()), nullable=False),
    pa.field("info_dp", pa.int32(), nullable=True),
    pa.field("info_ac", pa.list_(pa.int32()), nullable=True),
    pa.field("info_af", pa.list_(pa.float64()), nullable=True),
    pa.field("info_an", pa.int32(), nullable=True),
    pa.field("samples", pa.list_(SAMPLE_STRUCT_TYPE), nullable=False),
])
