"""
Tests for the Silver transformation and data-quality layers.

Test cases
----------
test_dedup_removes_duplicate_event
    After bronze_to_silver, event id '35000000001' (present twice in fixture)
    should appear only once.

test_merge_does_not_duplicate_on_rerun
    Running bronze_to_silver twice should produce the same row count as once —
    the MERGE INTO upsert is idempotent.

test_dq_catches_null_ids
    Injecting a row with a null id into the Silver table should cause
    run_dq_checks to raise DataQualityError (null_rate > threshold).

test_date_column_extracted_from_created_at
    The ``date`` column should match the date part of ``created_at``.

test_derived_columns_present
    Silver table must have: repo_name, actor_login, event_type, _processed_at.

test_silver_merge_updates_existing_row
    Sending an updated version of an existing event id should overwrite the
    row in Silver, not create a duplicate.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, StringType, StructField,
                                StructType, TimestampType)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FIXTURE = Path(__file__).parent / "fixtures" / "sample_events.json"


def _build_bronze(spark: SparkSession, tmp_path: Path) -> str:
    """Write the fixture data as a Bronze Delta table and return its path."""
    from src.bronze.ingest import _ingest_batch

    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    shutil.copy(str(FIXTURE), str(raw_dir / "sample_events.json"))

    bronze_path = str(tmp_path / "bronze")
    _ingest_batch(spark, str(raw_dir), bronze_path, ingested_at="2024-01-15T10:00:00+00:00")
    return bronze_path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSilverTransform:
    def test_dedup_removes_duplicate_event(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        """Event id 35000000001 appears twice in the fixture; Silver must deduplicate."""
        from src.silver.transform import bronze_to_silver

        bronze_path = _build_bronze(spark, tmp_path)
        silver_path = str(tmp_path / "silver")

        bronze_to_silver(spark, bronze_path=bronze_path, silver_path=silver_path)

        df = spark.read.format("delta").load(silver_path)
        count_35000000001 = df.filter(F.col("id") == "35000000001").count()
        assert count_35000000001 == 1, (
            f"Duplicate id should appear exactly once in Silver; found {count_35000000001}"
        )

    def test_merge_does_not_duplicate_on_rerun(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        """Running bronze_to_silver twice must be idempotent (MERGE upsert)."""
        from src.silver.transform import bronze_to_silver

        bronze_path = _build_bronze(spark, tmp_path)
        silver_path = str(tmp_path / "silver")

        bronze_to_silver(spark, bronze_path=bronze_path, silver_path=silver_path)
        first_count = spark.read.format("delta").load(silver_path).count()

        # Second run with identical Bronze data
        bronze_to_silver(spark, bronze_path=bronze_path, silver_path=silver_path)
        second_count = spark.read.format("delta").load(silver_path).count()

        assert first_count == second_count, (
            f"MERGE should be idempotent: first_count={first_count}, second_count={second_count}"
        )

    def test_derived_columns_present(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        """Silver table must contain all expected derived and audit columns."""
        from src.silver.transform import bronze_to_silver

        bronze_path = _build_bronze(spark, tmp_path)
        silver_path = str(tmp_path / "silver")

        bronze_to_silver(spark, bronze_path=bronze_path, silver_path=silver_path)

        df = spark.read.format("delta").load(silver_path)
        for col in ("repo_name", "actor_login", "event_type", "_processed_at", "date"):
            assert col in df.columns, f"Expected column '{col}' not found in Silver"

    def test_date_column_extracted_from_created_at(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        """date column should be the date part of created_at."""
        from src.silver.transform import bronze_to_silver

        bronze_path = _build_bronze(spark, tmp_path)
        silver_path = str(tmp_path / "silver")

        bronze_to_silver(spark, bronze_path=bronze_path, silver_path=silver_path)

        df = spark.read.format("delta").load(silver_path)
        # Filter to one known event and check date matches
        row = df.filter(F.col("id") == "35000000001").select("date", "created_at").first()
        assert row is not None, "Event 35000000001 should be in Silver"
        assert str(row["date"]) == "2024-01-15", (
            f"Expected date '2024-01-15', got '{row['date']}'"
        )

    def test_silver_merge_updates_existing_row(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        """
        Sending a modified version of an existing event should update the row
        in Silver, not create a duplicate.
        """
        from src.silver.transform import _add_derived_columns, _upsert_to_silver

        silver_path = str(tmp_path / "silver_update")

        # --- Initial write: event id 'X001' with type PushEvent ---
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("type", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("public", BooleanType(), True),
        ])
        initial = spark.createDataFrame(
            [("X001", "PushEvent", "2024-01-15T10:00:00", True)],
            schema=StructType([
                StructField("id", StringType(), True),
                StructField("type", StringType(), True),
                StructField("created_at", StringType(), True),
                StructField("public", BooleanType(), True),
            ]),
        ).withColumn("created_at", F.to_timestamp("created_at"))

        initial = _add_derived_columns(initial)
        _upsert_to_silver(spark, initial, silver_path)
        count_before = spark.read.format("delta").load(silver_path).count()

        # --- Second write: same id, different type ---
        updated = spark.createDataFrame(
            [("X001", "WatchEvent", "2024-01-15T10:00:00", True)],
            schema=StructType([
                StructField("id", StringType(), True),
                StructField("type", StringType(), True),
                StructField("created_at", StringType(), True),
                StructField("public", BooleanType(), True),
            ]),
        ).withColumn("created_at", F.to_timestamp("created_at"))

        updated = _add_derived_columns(updated)
        _upsert_to_silver(spark, updated, silver_path)

        df = spark.read.format("delta").load(silver_path)
        count_after = df.count()
        event_type = df.filter(F.col("id") == "X001").select("event_type").first()["event_type"]

        assert count_after == count_before, (
            "MERGE update should not increase row count"
        )
        assert event_type == "WatchEvent", (
            f"Event type should have been updated to WatchEvent, got '{event_type}'"
        )


class TestDataQuality:
    def test_dq_catches_null_ids(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        """A Silver table with > 5% null ids should raise DataQualityError."""
        from src.silver.dq import DataQualityError, run_dq_checks

        silver_path = str(tmp_path / "silver_bad")

        # Build a table with 10 rows, 6 of which have null id  → 60% null rate
        rows = [(None, "PushEvent")] * 6 + [
            ("id_1", "WatchEvent"),
            ("id_2", "WatchEvent"),
            ("id_3", "PushEvent"),
            ("id_4", "IssuesEvent"),
        ]
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("event_type", StringType(), True),
        ])
        bad_df = (
            spark.createDataFrame(rows, schema)
            .withColumn("created_at", F.to_timestamp(F.lit("2024-01-15T10:00:00")))
            .withColumn("date", F.to_date(F.lit("2024-01-15")))
            .withColumn("repo_name", F.lit("test/repo"))
            .withColumn("actor_login", F.lit("alice"))
            .withColumn("_processed_at", F.current_timestamp())
        )
        (
            bad_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(silver_path)
        )

        with pytest.raises(DataQualityError):
            run_dq_checks(
                spark,
                silver_path=silver_path,
                quarantine_path=str(tmp_path / "quarantine"),
                null_rate_threshold=0.05,
            )

    def test_dq_passes_clean_data(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        """A Silver table with zero null ids should pass all DQ checks."""
        from src.silver.dq import run_dq_checks
        from src.silver.transform import bronze_to_silver

        bronze_path = _build_bronze(spark, tmp_path)
        silver_path = str(tmp_path / "silver_clean")
        bronze_to_silver(spark, bronze_path=bronze_path, silver_path=silver_path)

        result = run_dq_checks(
            spark,
            silver_path=silver_path,
            quarantine_path=str(tmp_path / "quarantine"),
        )

        assert result["passed"] is True
        assert result["null_rate"] == 0.0
        assert result["row_count"] > 0

    def test_dq_quarantines_bad_records(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        """Bad records (null id) must be written to the quarantine path."""
        from src.silver.dq import run_dq_checks

        silver_path = str(tmp_path / "silver_mixed")
        quarantine_path = str(tmp_path / "quarantine")

        rows = [
            (None, "PushEvent"),  # bad — null id
            ("id_1", "WatchEvent"),
            ("id_2", "PushEvent"),
        ]
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("event_type", StringType(), True),
        ])
        df = (
            spark.createDataFrame(rows, schema)
            .withColumn("created_at", F.to_timestamp(F.lit("2024-01-15T10:00:00")))
            .withColumn("date", F.to_date(F.lit("2024-01-15")))
            .withColumn("repo_name", F.lit("test/repo"))
            .withColumn("actor_login", F.lit("alice"))
            .withColumn("_processed_at", F.current_timestamp())
        )
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_path)

        # null_rate = 1/3 ≈ 33% — will raise; catch it so we can check quarantine
        try:
            run_dq_checks(
                spark,
                silver_path=silver_path,
                quarantine_path=quarantine_path,
                null_rate_threshold=0.05,
            )
        except Exception:
            pass

        from delta.tables import DeltaTable
        assert DeltaTable.isDeltaTable(spark, quarantine_path), (
            "Quarantine Delta table should exist after bad records are found"
        )
        q_df = spark.read.format("delta").load(quarantine_path)
        assert q_df.count() == 1, "Exactly one bad record should be in quarantine"
