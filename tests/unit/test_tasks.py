"""Unit tests for direct VCF to Parquet conversion and sample ingestion."""
from unittest.mock import MagicMock, patch

import pyarrow.parquet as pq

from src.models import VARIANT_PARQUET_SCHEMA
from src.tasks.ingest_samples import ingest_sample_metadata_task
from src.tasks.vcf_to_parquet import vcf_record_to_dict, vcf_to_parquet_task


class TestVcfToParquet:
    """Test VCF extraction into Parquet format."""

    def test_vcf_record_to_dict_conversion(self):
        """Test converting a mock pysam record to dictionary matching schema."""
        mock_rec = MagicMock()
        mock_rec.chrom = "chr21"
        mock_rec.pos = 10000000
        mock_rec.id = "rs12345"
        mock_rec.ref = "C"
        mock_rec.alts = ("T",)
        mock_rec.qual = 99.5
        mock_rec.filter.keys.return_value = ["PASS"]
        mock_rec.info = {
            "DP": 50,
            "AC": (2,),
            "AF": (0.000312,),
            "AN": 6404,
        }

        # Mock sample call
        sample_call = {
            "GT": (0, 1),
            "GQ": 40,
            "DP": 15,
        }
        mock_rec.samples.get.return_value = sample_call
        mock_rec.samples.__getitem__.return_value = sample_call

        row = vcf_record_to_dict(mock_rec, ["HG00096"])

        assert row["chrom"] == "chr21"
        assert row["pos"] == 10000000
        assert row["ref"] == "C"
        assert row["alt"] == ["T"]
        assert row["qual"] == 99.5
        assert row["filter"] == ["PASS"]
        assert row["info_dp"] == 50
        assert row["info_ac"] == [2]
        assert row["info_af"] == [0.000312]
        assert row["info_an"] == 6404
        assert len(row["samples"]) == 1
        assert row["samples"][0]["sample_id"] == "HG00096"
        assert row["samples"][0]["gt"] == "0/1"
        assert row["samples"][0]["gq"] == 40
        assert row["samples"][0]["dp"] == 15

    def test_vcf_to_parquet_task_execution(self, tmp_path):
        """Test running vcf_to_parquet_task with mocked streamer."""
        mock_rec = MagicMock()
        mock_rec.chrom = "chr21"
        mock_rec.pos = 10000000
        mock_rec.id = None
        mock_rec.ref = "A"
        mock_rec.alts = ("G",)
        mock_rec.qual = 100.0
        mock_rec.filter.keys.return_value = ["PASS"]
        mock_rec.info = {"DP": 30, "AC": [1], "AF": [0.0005], "AN": 2000}
        mock_rec.samples.get.return_value = {"GT": (0, 0), "GQ": 30, "DP": 10}

        mock_header = MagicMock()
        mock_header.samples = ["HG00096", "HG00097"]

        with patch("src.tasks.vcf_to_parquet.create_streamer") as mock_create_streamer:
            streamer_instance = MagicMock()
            streamer_instance.get_header.return_value = mock_header
            streamer_instance.stream.return_value = [mock_rec]
            streamer_instance.__enter__.return_value = streamer_instance
            streamer_instance.__exit__.return_value = None
            mock_create_streamer.return_value = streamer_instance

            shards = vcf_to_parquet_task(
                source="https://fake.url/test.vcf.gz",
                region="chr21:10000000-10010000",
                lines_per_shard=10,
                storage_base_path=str(tmp_path),
                run_id="test_run",
            )

            assert len(shards) == 1
            parquet_path = tmp_path / shards[0]
            assert parquet_path.exists()

            # Verify Parquet schema and contents
            table = pq.read_table(str(parquet_path))
            assert table.schema == VARIANT_PARQUET_SCHEMA
            assert table.num_rows == 1
            assert table.column("chrom")[0].as_py() == "chr21"
            assert table.column("samples")[0].as_py()[0]["sample_id"] == "HG00096"


class TestSampleIngestion:
    """Test sample metadata ingestion."""

    def test_ingest_sample_metadata_local(self, tmp_path):
        """Test parsing and ingesting raw IGSR pedigree content."""
        sample_ped_content = (
            "FamilyID SampleID FatherID MotherID Sex Population Superpopulation\n"
            "HG00096 HG00096 0 0 1 GBR EUR\n"
            "HG00097 HG00097 0 0 2 GBR EUR\n"
        )
        src_file = tmp_path / "raw_ped.txt"
        src_file.write_text(sample_ped_content, encoding="utf-8")

        output_rel = ingest_sample_metadata_task(
            source_url=str(src_file),
            storage_base_path=str(tmp_path),
        )

        out_file = tmp_path / output_rel
        assert out_file.exists()
        lines = out_file.read_text(encoding="utf-8").splitlines()
        assert lines[0] == "family_id\tsample_id\tfather_id\tmother_id\tsex\tpopulation\tsuperpopulation"
        assert len(lines) == 3
        assert lines[1] == "HG00096\tHG00096\t0\t0\t1\tGBR\tEUR"
