"""
Silver layer — transform and upsert.

Responsibilities
----------------
* Read raw events from Bronze Delta table
* Apply the explicit GH_EVENT_SCHEMA
* Deduplicate on event id
* Add convenience columns: date, repo_name, actor_login, event_type
* Upsert into Silver Delta table via MERGE INTO (handles late data correctly)
* Attach ``_processed_at`` audit column

Public API
----------
    bronze_to_silver(spark, bronze_path, silver_path)
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from src.silver.schema import GH_EVENT_SCHEMA

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def bronze_to_silver(
    spark: SparkSession,
    bronze_path: Optional[str] = None,
    silver_path: Optional[str] = None,
) -> None:
    """
    Read the Bronze Delta table, apply schema, deduplicate, and upsert into
    the Silver Delta table.

    The Silver table is partitioned by ``date`` (derived from ``created_at``).
    MERGE INTO is used so that late-arriving duplicates are handled correctly:
    if a row with the same ``id`` already exists in Silver, it is updated;
    otherwise it is inserted.

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

    os.makedirs(silver_path, exist_ok=True)

    logger.info("[Silver] Reading Bronze table from %s", bronze_path)
    bronze_df = spark.read.format("delta").load(bronze_path)

    # The Bronze table stores events with Spark-inferred types (mostly strings).
    # We re-parse the JSON payload to apply strict types.  If the Bronze table
    # already stored structs (e.g. actor, repo), we fall back gracefully.
    silver_df = _apply_schema(spark, bronze_df)

    # Add convenience / audit columns
    silver_df = _add_derived_columns(silver_df)

    # Deduplicate within the incoming batch (keep the first occurrence)
    silver_df = silver_df.dropDuplicates(["id"])

    # Filter out rows with a null id — they cannot be merged meaningfully
    silver_df = silver_df.filter(F.col("id").isNotNull())

    logger.info("[Silver] Upserting into Silver table at %s", silver_path)
    _upsert_to_silver(spark, silver_df, silver_path)
    logger.info("[Silver] Transform complete.")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _apply_schema(spark: SparkSession, bronze_df: DataFrame) -> DataFrame:
    """
    Coerce the Bronze DataFrame to the canonical Silver schema.

    Strategy
    --------
    1. If Bronze contains a ``_raw_json`` column (written by an alternative
       ingest path), parse it with ``from_json`` using GH_EVENT_SCHEMA.
    2. Otherwise assume Bronze already has struct columns and cast individual
       fields to the expected types.
    """
    cols = set(bronze_df.columns)

    if "_raw_json" in cols:
        # Path 1 — raw JSON string column
        parsed = bronze_df.withColumn(
            "_parsed",
            F.from_json(F.col("_raw_json"), GH_EVENT_SCHEMA),
        )
        df = parsed.select("_parsed.*", "_ingested_at")
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
    Merge *incoming* DataFrame into the Silver Delta table.

    - If a row with the same ``id`` already exists → UPDATE all columns.
    - If no matching row exists → INSERT.

    This MERGE handles late-arriving events and pipeline reruns correctly.
    """
    if DeltaTable.isDeltaTable(spark, silver_path):
        silver_table = DeltaTable.forPath(spark, silver_path)

        (
            silver_table.alias("target")
            .merge(
                incoming.alias("source"),
                condition="target.id = source.id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        # First run — no existing table, just write.
        logger.info("[Silver] No existing Silver table found — performing initial write.")
        (
            incoming.write.format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .option("overwriteSchema", "true")
            .partitionBy("date")
            .save(silver_path)
        )
