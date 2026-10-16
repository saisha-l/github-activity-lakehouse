"""
Gold layer — incremental maintenance from the Silver Change Data Feed.

Instead of rebuilding every aggregate from 100M+ Silver rows on each run, a
Structured Streaming query reads Silver's Change Data Feed (CDF).  For each
micro-batch it:

1. collects the keys touched by the changed rows, per Gold table
   (``(date, repo_name)``, ``event_id``, ``repo_name``, ``actor_login``);
2. recomputes those keys' aggregates from the current Silver snapshot, using
   the same ``compute_*`` functions as the batch rebuild;
3. MERGEs the result into Gold with :func:`replace_scope` (update / insert /
   delete-stale in one atomic commit per table).

Step 2 is a pure function of Silver, so replaying a batch after a crash
rewrites identical values — the writes are idempotent and the checkpoint
gives exactly-once results.  If Silver has moved ahead of the batch (streams
running concurrently), the recompute already reflects the newer rows, and
their own CDF batch recomputes the same keys again — the tables converge.

On first start (no checkpoint) the CDF stream emits the full Silver snapshot
as inserts, so Gold is fully built without a separate backfill.

Public API
----------
    start_gold_stream(spark, silver_path, gold_path, checkpoint_path, ...)
    update_gold(spark, silver_path, gold_path, ...)  # blocking wrapper
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from src.gold.aggregations import (compute_contributor_retention,
                                   compute_daily_active_repos,
                                   compute_pr_merge_time,
                                   compute_top_growing_repos)
from src.utils.streaming import (build_trigger, partition_values,
                                 replace_scope)

logger = logging.getLogger(__name__)

_CDF_META_COLS = ["_change_type", "_commit_version", "_commit_timestamp"]


def start_gold_stream(
    spark: SparkSession,
    silver_path: Optional[str] = None,
    gold_path: Optional[str] = None,
    checkpoint_path: Optional[str] = None,
    trigger_interval: Optional[str] = None,
    max_files_per_trigger: Optional[int] = None,
) -> StreamingQuery:
    """Start the Silver-CDF → Gold stream and return the running query."""
    from conf.config import (DELTA_MAX_FILES_PER_TRIGGER, GOLD_CHECKPOINT,
                             GOLD_PATH, SILVER_PATH, STREAM_TRIGGER_INTERVAL)
    from src.silver.transform import ensure_silver_table

    silver_path = silver_path or SILVER_PATH
    gold_path = gold_path or GOLD_PATH
    checkpoint_path = checkpoint_path or GOLD_CHECKPOINT
    trigger_interval = trigger_interval if trigger_interval is not None else STREAM_TRIGGER_INTERVAL

    ensure_silver_table(spark, silver_path)

    changes = (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .option("maxFilesPerTrigger", max_files_per_trigger or DELTA_MAX_FILES_PER_TRIGGER)
        .load(silver_path)
    )

    logger.info("[Gold] Streaming CDF of %s → %s (checkpoint %s)", silver_path, gold_path, checkpoint_path)
    return (
        changes.writeStream.queryName("gold_incremental")
        .option("checkpointLocation", checkpoint_path)
        .foreachBatch(lambda df, batch_id: _write_gold_batch(df, batch_id, silver_path, gold_path))
        .trigger(**build_trigger(trigger_interval))
        .start()
    )


def update_gold(
    spark: SparkSession,
    silver_path: Optional[str] = None,
    gold_path: Optional[str] = None,
    checkpoint_path: Optional[str] = None,
    trigger_interval: Optional[str] = None,
) -> None:
    """Run :func:`start_gold_stream` and block until it stops."""
    start_gold_stream(
        spark,
        silver_path=silver_path,
        gold_path=gold_path,
        checkpoint_path=checkpoint_path,
        trigger_interval=trigger_interval,
    ).awaitTermination()
    logger.info("[Gold] Incremental update complete.")


# ---------------------------------------------------------------------------
# Micro-batch handler
# ---------------------------------------------------------------------------


def _write_gold_batch(
    batch_df: DataFrame,
    batch_id: int,
    silver_path: str,
    gold_path: str,
) -> None:
    spark = batch_df.sparkSession
    # Pre- and post-images both count: an update that moved an event to a
    # different repo/date must refresh the old key as well as the new one.
    changes = batch_df.drop(*_CDF_META_COLS).persist()
    try:
        if changes.isEmpty():
            return
        silver = spark.read.format("delta").load(silver_path)
        _update_daily_active_repos(spark, changes, silver, gold_path)
        _update_pr_merge_time(spark, changes, silver, gold_path)
        _update_top_growing_repos(spark, changes, silver, gold_path)
        _update_contributor_retention(spark, changes, silver, gold_path)
        logger.info("[Gold] batch %d applied", batch_id)
    finally:
        changes.unpersist()


def _update_daily_active_repos(
    spark: SparkSession, changes: DataFrame, silver: DataFrame, gold_path: str
) -> None:
    keys = (
        changes.select("date", "repo_name")
        .where(F.col("date").isNotNull() & F.col("repo_name").isNotNull())
        .distinct()
        .cache()
    )
    try:
        dates = partition_values(keys, "date")
        if not dates:
            return
        affected = silver.where(F.col("date").isin(dates)).join(
            keys, ["date", "repo_name"], "left_semi"
        )
        replace_scope(
            spark,
            compute_daily_active_repos(affected),
            os.path.join(gold_path, "daily_active_repos"),
            scope_keys=keys,
            scope_cols=["date", "repo_name"],
            merge_keys=["date", "repo_name"],
            partition_cols=["date"],
            prune_col="date",
        )
    finally:
        keys.unpersist()


def _update_pr_merge_time(
    spark: SparkSession, changes: DataFrame, silver: DataFrame, gold_path: str
) -> None:
    if "payload" not in silver.columns:
        return
    prs = changes.where(F.col("event_type") == "PullRequestEvent")
    keys = prs.select(F.col("id").alias("event_id"), "date").distinct().cache()
    try:
        dates = partition_values(keys, "date")
        if not dates:
            return
        affected = silver.where(F.col("date").isin(dates)).join(
            keys.select(F.col("event_id").alias("id")), ["id"], "left_semi"
        )
        replace_scope(
            spark,
            compute_pr_merge_time(affected),
            os.path.join(gold_path, "pr_merge_time"),
            scope_keys=keys,
            scope_cols=["event_id"],
            merge_keys=["event_id"],
            partition_cols=["date"],
            prune_col="date",
        )
    finally:
        keys.unpersist()


def _update_top_growing_repos(
    spark: SparkSession, changes: DataFrame, silver: DataFrame, gold_path: str
) -> None:
    keys = (
        changes.where((F.col("event_type") == "WatchEvent") & F.col("repo_name").isNotNull())
        .select("repo_name")
        .distinct()
        .cache()
    )
    try:
        if keys.isEmpty():
            return
        affected = silver.where(F.col("event_type") == "WatchEvent").join(
            keys, ["repo_name"], "left_semi"
        )
        replace_scope(
            spark,
            compute_top_growing_repos(affected),
            os.path.join(gold_path, "top_growing_repos"),
            scope_keys=keys,
            scope_cols=["repo_name"],
            merge_keys=["repo_name", "year", "week"],
        )
    finally:
        keys.unpersist()


def _update_contributor_retention(
    spark: SparkSession, changes: DataFrame, silver: DataFrame, gold_path: str
) -> None:
    keys = (
        changes.where(F.col("actor_login").isNotNull())
        .select("actor_login")
        .distinct()
        .cache()
    )
    try:
        if keys.isEmpty():
            return
        affected = silver.join(keys, ["actor_login"], "left_semi")
        replace_scope(
            spark,
            compute_contributor_retention(affected),
            os.path.join(gold_path, "contributor_retention"),
            scope_keys=keys,
            scope_cols=["actor_login"],
            merge_keys=["actor_login"],
        )
    finally:
        keys.unpersist()
