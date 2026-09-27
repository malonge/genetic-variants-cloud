"""
Task for ingesting IGSR sample pedigree and population metadata.

Fetches the 3,202-sample pedigree table from IGSR (EMBL-EBI) or a local file,
parses the tabular data, and saves it as a clean TSV in the Bronze layer
(data/bronze/samples/samples.tsv) ready for DuckDB external table querying.
"""
import logging
from pathlib import Path

import requests

from src.storage import create_storage

logger = logging.getLogger(__name__)

DEFAULT_IGSR_PED_URL = (
    "https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/data_collections/"
    "1000G_2504_high_coverage/20130606_g1k_3202_samples_ped_population.txt"
)


def ingest_sample_metadata_task(
    source_url: str = DEFAULT_IGSR_PED_URL,
    storage_type: str = "local",
    storage_base_path: str = "data",
    output_filename: str = "bronze/samples/samples.tsv",
) -> str:
    """
    Download and store IGSR sample metadata.

    Args:
        source_url: HTTPS URL or local file path to 20130606_g1k_3202_samples_ped_population.txt
        storage_type: 'local' or 'gcs'
        storage_base_path: Base directory for storage
        output_filename: Destination path relative to storage base

    Returns:
        Written file path
    """
    storage = create_storage(storage_type=storage_type, base_path=storage_base_path)
    logger.info(f"Ingesting sample metadata from: {source_url}")

    if source_url.startswith("http://") or source_url.startswith("https://"):
        resp = requests.get(source_url, timeout=30)
        resp.raise_for_status()
        raw_text = resp.text
    else:
        raw_text = Path(source_url).read_text(encoding="utf-8")

    lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
    if not lines:
        raise ValueError("Received empty metadata content")

    # Standardize header: family_id\tsample_id\tfather_id\tmother_id\tsex\tpopulation\tsuperpopulation
    # The source file is whitespace/tab-separated
    standardized_header = "\t".join([
        "family_id", "sample_id", "father_id", "mother_id", "sex", "population", "superpopulation"
    ])

    cleaned_rows = [standardized_header]
    for row in lines[1:]:
        tokens = row.split()
        if len(tokens) >= 7:
            cleaned_rows.append("\t".join(tokens[:7]))

    output_data = "\n".join(cleaned_rows) + "\n"
    written_path = storage.write_file(output_data.encode("utf-8"), output_filename)
    logger.info(f"Successfully wrote {len(cleaned_rows) - 1} sample records to {written_path}")

    return output_filename


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    parser = argparse.ArgumentParser(description="Ingest 1000 Genomes sample metadata")
    parser.add_argument("--source", default=DEFAULT_IGSR_PED_URL, help="URL or path to IGSR ped file")
    parser.add_argument("--storage-base-path", default="data")

    args = parser.parse_args()
    ingest_sample_metadata_task(source_url=args.source, storage_base_path=args.storage_base_path)
