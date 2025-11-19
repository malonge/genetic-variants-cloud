"""
Core data models for the pipeline.

Note: For streaming/sharding, we use pysam.VariantRecord directly.
Future transformation stages (TSV, Avro) will define their own schemas here.
"""

# Placeholder for future transformation schemas
# Example:
# class TSVVariantRecord(BaseModel):
#     """Schema for flattened TSV variant data"""
#     pass
#
# class AvroVariantRecord(BaseModel):
#     """Schema for Avro variant data"""
#     pass
