"""
Bronze layer — raw, append-only ingestion from GH Archive.

Responsibilities
----------------
* Download hourly .json.gz files from https://data.gharchive.org/
* Incrementally ingest new files with Spark Structured Streaming
  (file source + checkpoint, so each file is processed exactly once)
* Keep every event verbatim in ``_raw_json`` (no schema-on-write, no data loss
  when GitHub adds fields) and extract only the keys needed for dedup and
  partitioning: ``id``, ``type``, ``created_at``, ``date``
* Write with an insert-only MERGE on ``id`` → duplicates across files, reruns
  and replayed micro-batches never produce duplicate rows
* Route unparseable lines (no id / created_at) to a quarantine table

Public API
----------
    download_gharchive(date_str, hour)  -> local file path  (re-exported from download.py)
    ingest_to_bronze(spark, raw_path, bronze_path, checkpoint_path, use_streaming)
    start_bronze_stream(spark, ...)     -> StreamingQuery
"""

from __future__ import annotations

import logging
import os
import re
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (DateType, StringType, StructField, StructType,
                               TimestampType)

from src.bronze.download import download_gharchive  # noqa: F401  (public API)
from src.utils.streaming import (build_trigger, ensure_delta_table,
                                 idempotent_append, merge_insert_only)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
# Regex to pull the date from the filename, e.g. "2024-01-15-3.json.gz"
_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})-\d+\.json(?:\.gz)?$")


# ---------------------------------------------------------------------------
# Bronze table layout
# ---------------------------------------------------------------------------

BRONZE_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("_raw_json", StringType(), True),
        StructField("_source_file", StringType(), True),
        StructField("_ingested_at", TimestampType(), True),
        StructField("date", DateType(), True),
    ]
)

_QUARANTINE_APP_ID = "bronze-quarantine"


def _date_from_path(path: str) -> Optional[str]:
    """Extract the date portion from a GH Archive filename."""
    match = _DATE_RE.search(os.path.basename(path))
    return match.group(1) if match else None


def _parse_raw_lines(lines: DataFrame) -> DataFrame:
    """
    Turn raw NDJSON lines (``value`` column) into Bronze rows.

    Only the dedup / partition keys are extracted; the full event stays in
    ``_raw_json`` and is parsed with the strict schema in Silver.
    """
    return (
        lines.filter(F.length(F.trim(F.col("value"))) > 0)
        .select(
            F.get_json_object("value", "$.id").alias("id"),
            F.get_json_object("value", "$.type").alias("type"),
            F.to_timestamp(F.get_json_object("value", "$.created_at")).alias("created_at"),
            F.col("value").alias("_raw_json"),
            F.input_file_name().alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"),
        )
        .withColumn("date", F.to_date("created_at"))
    )


def _checkpoint_query_id(spark: SparkSession, checkpoint_path: str) -> str:
    """
    The stream's persistent id from ``<checkpoint>/metadata``.

    Scoping ``txnAppId`` to it means a fresh checkpoint (batch ids restarting
    at 0) gets a fresh transaction id instead of having its writes skipped.
    """
    import json

    meta = spark.read.text(os.path.join(checkpoint_path, "metadata")).first()[0]
    return json.loads(meta)["id"]


def _write_bronze_batch(
    batch_df: DataFrame,
    batch_id: Optional[int],
    bronze_path: str,
    quarantine_path: str,
    txn_app_id: Optional[str] = None,
) -> None:
    """
    Idempotently write one micro-batch (or one batch run) to Bronze.

    * Valid rows → insert-only MERGE on ``id`` (pruned to the batch's dates).
    * Invalid rows → quarantine; in streaming mode the append is tagged with
      ``txnAppId``/``txnVersion`` so a replayed batch is skipped by Delta.
    """
    spark = batch_df.sparkSession
    batch_df = batch_df.persist()
    try:
        valid = F.col("id").isNotNull() & F.col("created_at").isNotNull()

        good = batch_df.filter(valid).dropDuplicates(["id"])
        merge_insert_only(spark, good, bronze_path, keys=["id"], prune_col="date")

        bad = batch_df.filter(~valid).withColumn(
            "_quarantine_reason",
            F.when(F.col("id").isNull(), F.lit("null_id")).otherwise(F.lit("null_created_at")),
        )
        if not bad.isEmpty():
            if batch_id is None or txn_app_id is None:
                bad.write.format("delta").mode("append").option("mergeSchema", "true").save(
                    quarantine_path
                )
            else:
                idempotent_append(bad, quarantine_path, txn_app_id, batch_id)
            logger.warning("[Bronze] batch %s: quarantined malformed rows", batch_id)
    finally:
        batch_df.unpersist()


def _ensure_bronze_table(spark: SparkSession, bronze_path: str) -> None:
    ensure_delta_table(spark, bronze_path, BRONZE_SCHEMA, partition_cols=["date"])


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def ingest_to_bronze(
    spark: SparkSession,
    raw_path: Optional[str] = None,
    bronze_path: Optional[str] = None,
    checkpoint_path: Optional[str] = None,
    use_streaming: bool = True,
    quarantine_path: Optional[str] = None,
    trigger_interval: Optional[str] = None,
) -> None:
    """
    Ingest raw GH Archive files into the Bronze Delta table.

    * **Streaming** (default): Structured Streaming file source with a
      checkpoint — only files not yet recorded in the checkpoint are read.
      Blocks until the stream stops (``availableNow``) or fails.
    * **Batch** (``use_streaming=False``): re-reads every file under
      *raw_path*.  Still idempotent thanks to the insert-only MERGE, so it is
      safe for backfills and repairs.
    """
    from conf.config import BRONZE_PATH, BRONZE_QUARANTINE_PATH, RAW_PATH

    raw_path = raw_path or RAW_PATH
    bronze_path = bronze_path or BRONZE_PATH
    quarantine_path = quarantine_path or BRONZE_QUARANTINE_PATH

    if use_streaming:
        query = start_bronze_stream(
            spark,
            raw_path=raw_path,
            bronze_path=bronze_path,
            checkpoint_path=checkpoint_path,
            quarantine_path=quarantine_path,
            trigger_interval=trigger_interval,
        )
        query.awaitTermination()
        logger.info("[Bronze] Streaming ingestion complete.")
    else:
        _ingest_batch(spark, raw_path, bronze_path, quarantine_path=quarantine_path)


def start_bronze_stream(
    spark: SparkSession,
    raw_path: Optional[str] = None,
    bronze_path: Optional[str] = None,
    checkpoint_path: Optional[str] = None,
    quarantine_path: Optional[str] = None,
    trigger_interval: Optional[str] = None,
    max_files_per_trigger: Optional[int] = None,
) -> StreamingQuery:
    """
    Start the raw-files → Bronze stream and return the running query.

    ``maxFilesPerTrigger`` bounds each micro-batch (GH Archive files are gzip,
    i.e. one non-splittable task per file), keeping memory flat no matter how
    large the backlog is — a month-long backfill of 120M+ events runs as a
    sequence of ~1-day micro-batches, each committed to the checkpoint.
    """
    from conf.config import (BRONZE_CHECKPOINT, BRONZE_PATH,
                             BRONZE_QUARANTINE_PATH, MAX_FILES_PER_TRIGGER,
                             RAW_PATH, STREAM_TRIGGER_INTERVAL)

    raw_path = raw_path or RAW_PATH
    bronze_path = bronze_path or BRONZE_PATH
    checkpoint_path = checkpoint_path or BRONZE_CHECKPOINT
    quarantine_path = quarantine_path or BRONZE_QUARANTINE_PATH
    trigger_interval = trigger_interval if trigger_interval is not None else STREAM_TRIGGER_INTERVAL

    if "://" not in raw_path:
        os.makedirs(raw_path, exist_ok=True)
    _ensure_bronze_table(spark, bronze_path)

    lines = (
        spark.readStream.format("text")
        .option("maxFilesPerTrigger", max_files_per_trigger or MAX_FILES_PER_TRIGGER)
        .load(raw_path)
    )

    app_id: dict = {}

    def write_batch(df: DataFrame, batch_id: int) -> None:
        if "id" not in app_id:
            query_id = _checkpoint_query_id(df.sparkSession, checkpoint_path)
            app_id["id"] = f"{_QUARANTINE_APP_ID}-{query_id}"
        _write_bronze_batch(df, batch_id, bronze_path, quarantine_path, app_id["id"])

    logger.info("[Bronze] Streaming %s → %s (checkpoint %s)", raw_path, bronze_path, checkpoint_path)
    return (
        _parse_raw_lines(lines)
        .writeStream.queryName("bronze_ingest")
        .option("checkpointLocation", checkpoint_path)
        .foreachBatch(write_batch)
        .trigger(**build_trigger(trigger_interval))
        .start()
    )


def _ingest_batch(
    spark: SparkSession,
    raw_path: str,
    bronze_path: str,
    ingested_at: Optional[str] = None,
    quarantine_path: Optional[str] = None,
) -> None:
    """
    Batch (non-streaming) Bronze ingestion of every file under *raw_path*.

    ``ingested_at`` is accepted for backwards compatibility and ignored —
    ``_ingested_at`` is always the actual write time.
    """
    # Default the quarantine next to the Bronze table so ad-hoc runs (and
    # tests using tmp dirs) never write into the main lakehouse.
    quarantine_path = quarantine_path or os.path.join(
        os.path.dirname(bronze_path.rstrip("/")), "quarantine", "bronze"
    )

    logger.info("[Bronze] Batch read from %s", raw_path)
    _ensure_bronze_table(spark, bronze_path)
    lines = spark.read.format("text").load(raw_path)
    _write_bronze_batch(_parse_raw_lines(lines), None, bronze_path, quarantine_path)
    logger.info("[Bronze] Batch ingestion complete.")
