"""
Pytest configuration and shared fixtures for the GitHub Activity Lakehouse tests.

Fixtures
--------
spark           (session scope) — local SparkSession with Delta Lake extensions
sample_df       (function scope) — DataFrame loaded from tests/fixtures/sample_events.json
bronze_df       (function scope) — sample_df with bronze audit columns added
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Ensure the project root is on sys.path so that ``conf`` and ``src`` imports
# resolve correctly regardless of how pytest is invoked.
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

FIXTURES_DIR = PROJECT_ROOT / "tests" / "fixtures"


# ---------------------------------------------------------------------------
# SparkSession fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Session-scoped local SparkSession with Delta Lake support.

    Using session scope means the JVM is started only once per test run,
    keeping test times manageable.
    """
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("lakehouse-tests")
        .master("local[2]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")  # deterministic for tests
        .config("spark.delta.logStore.class",
                "org.apache.spark.sql.delta.storage.HDFSLogStore")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


# ---------------------------------------------------------------------------
# Sample DataFrame fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="function")
def sample_df(spark: SparkSession) -> DataFrame:
    """
    DataFrame loaded from tests/fixtures/sample_events.json.

    The fixture contains 10 lines of newline-delimited JSON representing
    realistic GH Archive events (including one intentional duplicate).
    """
    fixture_path = str(FIXTURES_DIR / "sample_events.json")
    df = (
        spark.read.option("multiLine", "false")
        .option("mode", "PERMISSIVE")
        .json(fixture_path)
    )
    return df


# ---------------------------------------------------------------------------
# Bronze DataFrame fixture (sample_df + audit columns)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="function")
def bronze_df(sample_df: DataFrame) -> DataFrame:
    """
    sample_df with bronze audit columns (_ingested_at, date) applied.
    Mirrors what ingest.py produces.
    """
    df = sample_df.withColumn("_ingested_at", F.current_timestamp())
    df = df.withColumn("date", F.to_date(F.to_timestamp(F.col("created_at"))))
    return df


# ---------------------------------------------------------------------------
# Helper: write a DataFrame as a Delta table to a temp path
# ---------------------------------------------------------------------------


def write_delta(df: DataFrame, path: str, partition_col: str = "date") -> None:
    """Write *df* as a Delta table to *path*, partitioned by *partition_col*."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .option("overwriteSchema", "true")
        .partitionBy(partition_col)
        .save(path)
    )
