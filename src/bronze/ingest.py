"""
Bronze layer — raw ingestion from GH Archive.

Responsibilities
----------------
* Download hourly .json.gz files from https://data.gharchive.org/
* Read raw NDJSON → Spark DataFrame (streaming or batch)
* Write events as-is (one JSON string per row) to Delta Lake, partitioned by date
* Attach ``_ingested_at`` audit column

Public API
----------
    download_gharchive(date_str, hour)  -> local file path
    ingest_to_bronze(spark, raw_path, bronze_path, checkpoint_path)
"""

from __future__ import annotations

import gzip
import logging
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_GHARCHIVE_URL_TEMPLATE = "https://data.gharchive.org/{date}-{hour}.json.gz"
# Regex to pull the date from the filename, e.g. "2024-01-15-3.json.gz"
_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})-\d+\.json(?:\.gz)?$")


# ---------------------------------------------------------------------------
# Download helper
# ---------------------------------------------------------------------------

def download_gharchive(
    date_str: str,
    hour: int,
    raw_path: Optional[str] = None,
    overwrite: bool = False,
) -> str:
    """
    Download one hourly GH Archive file.

    Parameters
    ----------
    date_str:
        ISO date string, e.g. ``"2024-01-15"``.
    hour:
        Hour 0–23.
    raw_path:
        Local directory to save the file.  Defaults to ``data/raw``.
    overwrite:
        When False (default) skips download if the file already exists.

    Returns
    -------
    str
        Absolute local path of the downloaded (or pre-existing) file.
    """
    from conf.config import RAW_PATH

    raw_path = raw_path or RAW_PATH
    os.makedirs(raw_path, exist_ok=True)

    filename = f"{date_str}-{hour}.json.gz"
    dest = os.path.join(raw_path, filename)

    if os.path.exists(dest) and not overwrite:
        logger.info("Already exists, skipping: %s", dest)
        return dest

    url = _GHARCHIVE_URL_TEMPLATE.format(date=date_str, hour=hour)
    logger.info("Downloading %s → %s", url, dest)

    with requests.get(url, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as fh:
            shutil.copyfileobj(resp.raw, fh)

    logger.info("Saved %s (%.1f MB)", dest, os.path.getsize(dest) / 1_048_576)
    return dest


# ---------------------------------------------------------------------------
# Bronze ingestion — streaming (with batch fallback)
# ---------------------------------------------------------------------------

# Minimal schema for the raw read: we keep every field as a raw JSON string so
# that downstream Silver can parse with the full schema without schema-on-write
# constraints.  We do extract the top-level ``id`` and ``created_at`` fields so
# we can derive a partition date cheaply.

_BRONZE_RAW_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("public", StringType(), True),
        # Everything else lands verbatim via the wildcard — we store the whole
        # row as a single JSON string for maximum flexibility.
    ]
)


def _date_from_path(path: str) -> Optional[str]:
    """Extract the date portion from a GH Archive filename."""
    match = _DATE_RE.search(os.path.basename(path))
    return match.group(1) if match else None


def _build_bronze_df(spark: SparkSession, raw_path: str) -> DataFrame:
    """
    Read all .json.gz files under *raw_path* as a Spark batch DataFrame.

    Each source file is newline-delimited JSON (one GH event per line).
    We read every top-level column Spark infers and then add audit columns.
    """
    df = (
        spark.read.option("multiLine", "false")
        .option("mode", "PERMISSIVE")
        .json(raw_path)
    )
    return df


def ingest_to_bronze(
    spark: SparkSession,
    raw_path: Optional[str] = None,
    bronze_path: Optional[str] = None,
    checkpoint_path: Optional[str] = None,
    use_streaming: bool = False,
) -> None:
    """
    Read raw GH Archive JSON files and write them to the Bronze Delta table.

    The function supports two modes:

    * **Batch** (default, ``use_streaming=False``): reads all files already
      present in *raw_path*, deduplicates on ``id``, and overwrites/merges
      into the Bronze Delta table.  Suitable for backfills and CI.

    * **Streaming** (``use_streaming=True``): uses Spark Structured Streaming
      with *cloudFiles*-compatible trigger.  Suitable for near-real-time
      ingestion on Databricks Auto Loader or vanilla Spark.

    In both cases:
    * Adds ``_ingested_at`` (current UTC timestamp).
    * Adds ``date`` partition column derived from ``created_at``.
    * Writes with ``mergeSchema=true``.

    Parameters
    ----------
    spark:
        Active SparkSession (must have Delta extensions).
    raw_path:
        Directory containing ``.json.gz`` source files.
    bronze_path:
        Delta table destination path.
    checkpoint_path:
        Checkpoint location for Structured Streaming.  Ignored in batch mode.
    use_streaming:
        Use Structured Streaming instead of batch read.
    """
    from conf.config import (
        BRONZE_PATH,
        CHECKPOINT_PATH,
        MAX_FILES_PER_TRIGGER,
        RAW_PATH,
    )

    raw_path = raw_path or RAW_PATH
    bronze_path = bronze_path or BRONZE_PATH
    checkpoint_path = checkpoint_path or os.path.join(CHECKPOINT_PATH, "bronze")

    os.makedirs(bronze_path, exist_ok=True)
    os.makedirs(checkpoint_path, exist_ok=True)

    ingested_at = datetime.now(tz=timezone.utc).isoformat()

    if use_streaming:
        _ingest_streaming(spark, raw_path, bronze_path, checkpoint_path, ingested_at)
    else:
        _ingest_batch(spark, raw_path, bronze_path, ingested_at)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _add_audit_columns(df: DataFrame) -> DataFrame:
    """Attach ``_ingested_at`` and derive the ``date`` partition column."""
    df = df.withColumn(
        "_ingested_at",
        F.current_timestamp(),
    )
    # created_at arrives as a string like "2024-01-15T10:00:00Z"
    df = df.withColumn(
        "date",
        F.to_date(F.col("created_at").cast("timestamp")),
    )
    return df


def _ingest_batch(
    spark: SparkSession,
    raw_path: str,
    bronze_path: str,
    ingested_at: str,
) -> None:
    """Batch (non-streaming) bronze ingestion."""
    logger.info("[Bronze] Batch read from %s", raw_path)

    df = _build_bronze_df(spark, raw_path)
    df = _add_audit_columns(df)

    # Drop rows that arrived completely empty (Spark reads empty lines as
    # all-null rows from some gzip files).
    df = df.filter(F.col("id").isNotNull())

    # Deduplicate within the current batch on event id.
    df = df.dropDuplicates(["id"])

    logger.info("[Bronze] Writing to Delta at %s", bronze_path)
    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("date")
        .save(bronze_path)
    )
    logger.info("[Bronze] Batch ingestion complete.")


def _ingest_streaming(
    spark: SparkSession,
    raw_path: str,
    bronze_path: str,
    checkpoint_path: str,
    ingested_at: str,
) -> None:
    """
    Streaming bronze ingestion using Spark Structured Streaming.

    Uses a ``maxFilesPerTrigger`` trigger so the stream processes a bounded
    number of files per micro-batch, making it easy to control throughput.
    The stream runs until all currently-available files are processed
    (``trigger(availableNow=True)``).
    """
    from conf.config import MAX_FILES_PER_TRIGGER

    logger.info("[Bronze] Streaming read from %s", raw_path)

    stream_df = (
        spark.readStream.format("json")
        .option("multiLine", "false")
        .option("mode", "PERMISSIVE")
        .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .load(raw_path)
    )

    stream_df = _add_audit_columns(stream_df)

    query = (
        stream_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .partitionBy("date")
        .trigger(availableNow=True)
        .start(bronze_path)
    )

    query.awaitTermination()
    logger.info("[Bronze] Streaming ingestion complete.")
