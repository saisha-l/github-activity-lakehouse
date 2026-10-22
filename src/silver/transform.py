"""
Silver layer — typed, deduplicated events, maintained incrementally.

Responsibilities
----------------
* Stream new Bronze rows (Delta streaming source + checkpoint)
* Parse ``_raw_json`` with the explicit GH_EVENT_SCHEMA
* Add convenience columns: date, repo_name, actor_login, event_type
* Upsert into Silver via MERGE on ``id`` (pruned to the batch's dates) —
  replays and late duplicates never create extra rows
* Publish row-level changes through Delta Change Data Feed so Gold can update
  only the aggregates that were affected

Public API
----------
    bronze_to_silver(spark, bronze_path, silver_path, use_streaming=False)
    start_silver_stream(spark, ...)  -> StreamingQuery
"""

from __future__ import annotations

import logging
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

from src.silver.schema import GH_EVENT_SCHEMA
from src.utils.streaming import build_trigger, ensure_delta_table, merge_upsert

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


SILVER_TABLE_PROPERTIES = {"delta.enableChangeDataFeed": "true"}


def bronze_to_silver(
    spark: SparkSession,
    bronze_path: Optional[str] = None,
    silver_path: Optional[str] = None,
    use_streaming: bool = False,
    checkpoint_path: Optional[str] = None,
    trigger_interval: Optional[str] = None,
) -> None:
    """
    Bring Silver up to date with Bronze.

    * **Batch** (default): read the whole Bronze table and MERGE it into
      Silver.  Idempotent, so it doubles as a repair / full-rebuild tool.
    * **Streaming** (``use_streaming=True``): process only Bronze commits not
      yet recorded in the Silver checkpoint, then stop (``availableNow``) or
      keep running (processing-time trigger).

    Parameters
    ----------
    spark:
        Active SparkSession with Delta extensions configured.
    bronze_path:
        Source Delta table path (Bronze layer).
    silver_path:
        Target Delta table path (Silver layer).
    """
    from conf.config import BRONZE_PATH, SILVER_PATH

    bronze_path = bronze_path or BRONZE_PATH
    silver_path = silver_path or SILVER_PATH

    if use_streaming:
        query = start_silver_stream(
            spark,
            bronze_path=bronze_path,
            silver_path=silver_path,
            checkpoint_path=checkpoint_path,
            trigger_interval=trigger_interval,
        )
        query.awaitTermination()
        logger.info("[Silver] Streaming transform complete.")
        return

    logger.info("[Silver] Reading Bronze table from %s", bronze_path)
    bronze_df = spark.read.format("delta").load(bronze_path)

    logger.info("[Silver] Upserting into Silver table at %s", silver_path)
    _upsert_to_silver(spark, _to_silver_rows(spark, bronze_df), silver_path)
    logger.info("[Silver] Transform complete.")


def start_silver_stream(
    spark: SparkSession,
    bronze_path: Optional[str] = None,
    silver_path: Optional[str] = None,
    checkpoint_path: Optional[str] = None,
    trigger_interval: Optional[str] = None,
    max_files_per_trigger: Optional[int] = None,
) -> StreamingQuery:
    """
    Start the Bronze → Silver stream and return the running query.

    Bronze is written with insert-only MERGEs, which Delta commits as pure
    appends, so it is a valid append-only streaming source.  OPTIMIZE
    commits (``dataChange=false``) are skipped automatically.
    """
    from conf.config import (BRONZE_PATH, SILVER_MAX_FILES_PER_TRIGGER,
                             SILVER_CHECKPOINT, SILVER_PATH,
                             STREAM_TRIGGER_INTERVAL)
    from src.bronze.ingest import _ensure_bronze_table

    bronze_path = bronze_path or BRONZE_PATH
    silver_path = silver_path or SILVER_PATH
    checkpoint_path = checkpoint_path or SILVER_CHECKPOINT
    trigger_interval = trigger_interval if trigger_interval is not None else STREAM_TRIGGER_INTERVAL

    _ensure_bronze_table(spark, bronze_path)
    ensure_silver_table(spark, silver_path)

    source = (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", max_files_per_trigger or SILVER_MAX_FILES_PER_TRIGGER)
        .load(bronze_path)
    )

    logger.info("[Silver] Streaming %s → %s (checkpoint %s)", bronze_path, silver_path, checkpoint_path)
    return (
        source.writeStream.queryName("silver_transform")
        .option("checkpointLocation", checkpoint_path)
        .foreachBatch(lambda df, _batch_id: _write_silver_batch(df, silver_path))
        .trigger(**build_trigger(trigger_interval))
        .start()
    )


def ensure_silver_table(spark: SparkSession, silver_path: str) -> None:
    """Create the Silver table (typed schema, CDF on) if it does not exist."""
    from src.bronze.ingest import BRONZE_SCHEMA

    schema = _to_silver_rows(spark, spark.createDataFrame([], BRONZE_SCHEMA)).schema
    ensure_delta_table(
        spark, silver_path, schema,
        partition_cols=["date"],
        properties=SILVER_TABLE_PROPERTIES,
    )


def _write_silver_batch(batch_df: DataFrame, silver_path: str) -> None:
    """
    Idempotently MERGE one Bronze micro-batch into Silver.

    No ``txnVersion`` bookkeeping is needed: a replayed batch matches every
    row on ``id`` and the update condition (strictly newer ``_ingested_at``)
    is false, so nothing changes.
    """
    spark = batch_df.sparkSession
    # Cache the raw batch so the source is scanned once, even though the
    # MERGE and the partition-pruning lookup both consume it.
    batch_df = batch_df.persist()
    try:
        _upsert_to_silver(spark, _to_silver_rows(spark, batch_df), silver_path)
    finally:
        batch_df.unpersist()


def _to_silver_rows(spark: SparkSession, bronze_df: DataFrame) -> DataFrame:
    """Bronze rows → typed, derived, deduplicated Silver rows."""
    df = _apply_schema(spark, bronze_df)
    df = _add_derived_columns(df)
    df = df.filter(F.col("id").isNotNull())
    if "_ingested_at" not in df.columns:
        return df.dropDuplicates(["id"])
    # Keep the most recently ingested copy of each event (deterministic,
    # unlike dropDuplicates, so a replayed batch picks the same row).
    w = Window.partitionBy("id").orderBy(F.col("_ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _apply_schema(spark: SparkSession, bronze_df: DataFrame) -> DataFrame:
    """
    Coerce the Bronze DataFrame to the canonical Silver schema.

    Strategy
    --------
    1. Bronze rows carry the verbatim event in ``_raw_json`` (the standard
       ingest path) — parse it with ``from_json`` using GH_EVENT_SCHEMA.
    2. Legacy Bronze tables with Spark-inferred struct columns — cast the
       top-level scalars to the expected types.
    """
    cols = set(bronze_df.columns)

    if "_raw_json" in cols:
        # Path 1 — raw JSON string column
        parsed = bronze_df.withColumn(
            "_parsed",
            F.from_json(F.col("_raw_json"), GH_EVENT_SCHEMA),
        )
        extra = [F.col("_ingested_at")] if "_ingested_at" in cols else []
        if "created_at" in cols:
            extra.append(F.col("created_at").cast("timestamp").alias("_bronze_created_at"))
        df = parsed.select("_parsed.*", *extra)
        # Bronze already resolved created_at; use it if the strict parse failed.
        if "_bronze_created_at" in df.columns:
            df = df.withColumn(
                "created_at", F.coalesce(F.col("created_at"), F.col("_bronze_created_at"))
            ).drop("_bronze_created_at")
    else:
        # Path 2 — Bronze already has typed / semi-typed columns.
        # Cast the top-level scalars; keep structs as-is (Delta mergeSchema
        # will handle type widening).
        df = bronze_df

        # Ensure id is a string
        if "id" in cols:
            df = df.withColumn("id", F.col("id").cast(StringType()))

        # Ensure created_at is a timestamp
        if "created_at" in cols:
            try:
                df = df.withColumn(
                    "created_at",
                    F.to_timestamp(F.col("created_at")),
                )
            except Exception:
                pass  # already a timestamp

    return df


def _add_derived_columns(df: DataFrame) -> DataFrame:
    """Add computed helper columns and the _processed_at audit column."""
    # Flatten nested struct fields into top-level convenience columns
    if "created_at" in df.columns:
        df = df.withColumn("date", F.to_date(F.col("created_at")))
    else:
        df = df.withColumn("date", F.lit(None).cast("date"))

    # repo_name — safe navigation in case repo struct is null
    if "repo" in df.columns:
        df = df.withColumn(
            "repo_name",
            F.when(F.col("repo").isNotNull(), F.col("repo.name")).otherwise(F.lit(None)),
        )
    else:
        df = df.withColumn("repo_name", F.lit(None).cast(StringType()))

    # actor_login — safe navigation
    if "actor" in df.columns:
        df = df.withColumn(
            "actor_login",
            F.when(F.col("actor").isNotNull(), F.col("actor.login")).otherwise(F.lit(None)),
        )
    else:
        df = df.withColumn("actor_login", F.lit(None).cast(StringType()))

    # event_type — alias for type (type is a reserved keyword in some contexts)
    if "type" in df.columns:
        df = df.withColumn("event_type", F.col("type"))
    else:
        df = df.withColumn("event_type", F.lit(None).cast(StringType()))

    # Audit timestamp
    df = df.withColumn("_processed_at", F.current_timestamp())

    return df


def _upsert_to_silver(
    spark: SparkSession,
    incoming: DataFrame,
    silver_path: str,
) -> None:
    """
    Merge *incoming* into the Silver Delta table.

    - Matching ``id`` → UPDATE, but only when the incoming copy was ingested
      later than the stored one (so replays are no-ops and stale copies never
      overwrite newer data).  Without ``_ingested_at`` the update is
      unconditional.
    - No matching row → INSERT.
    - The target scan is pruned to the ``date`` partitions in the batch.
    """
    if not DeltaTable.isDeltaTable(spark, silver_path):
        # First run — no existing table, just write.
        logger.info("[Silver] No existing Silver table found — performing initial write.")
        (
            incoming.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("date")
            .save(silver_path)
        )
        ensure_delta_table(
            spark, silver_path, incoming.schema, properties=SILVER_TABLE_PROPERTIES
        )
        return

    target_cols = set(spark.read.format("delta").load(silver_path).columns)
    condition = None
    if "_ingested_at" in incoming.columns and "_ingested_at" in target_cols:
        condition = "t._ingested_at IS NULL OR s._ingested_at > t._ingested_at"

    merge_upsert(
        spark, incoming, silver_path,
        keys=["id"],
        prune_col="date",
        update_condition=condition,
    )
