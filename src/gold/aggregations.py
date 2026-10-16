"""
Gold layer — aggregated analytical tables.

Each table is defined once by a pure ``compute_*`` function (Silver rows in,
Gold rows out).  Two writers share those definitions:

* the batch functions below — full rebuild, overwrite (idempotent);
* ``src/gold/incremental.py`` — a Change Data Feed stream that recomputes
  only the keys touched by each micro-batch and MERGEs them in.

Gold tables produced
--------------------
1. daily_active_repos       — events per repo per day + distinct actor count
2. pr_merge_time            — hours-to-merge for merged pull requests
3. top_growing_repos        — WatchEvent (star) week-over-week growth
4. contributor_retention    — actors active in 2+ consecutive weeks (retained)

Public API
----------
    compute_daily_active_repos(df) / compute_pr_merge_time(df) /
    compute_top_growing_repos(df) / compute_contributor_retention(df)
    daily_active_repos(spark, silver_path, gold_path)
    pr_merge_time(spark, silver_path, gold_path)
    top_growing_repos(spark, silver_path, gold_path)
    contributor_retention(spark, silver_path, gold_path)
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _read_silver(spark: SparkSession, silver_path: str) -> DataFrame:
    return spark.read.format("delta").load(silver_path)


def _write_gold(df: DataFrame, path: str, partition_cols: Optional[list] = None) -> None:
    os.makedirs(path, exist_ok=True)
    writer = (
        df.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .option("overwriteSchema", "true")
    )
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)
    logger.info("[Gold] Written to %s", path)


# ---------------------------------------------------------------------------
# 1. Daily active repos
# ---------------------------------------------------------------------------


def daily_active_repos(
    spark: SparkSession,
    silver_path: Optional[str] = None,
    gold_path: Optional[str] = None,
) -> DataFrame:
    """
    Compute per-repository, per-day event counts and distinct actor counts.

    Schema
    ------
    date | repo_name | event_count | distinct_actors | last_updated_at
    """
    from conf.config import GOLD_DAILY_ACTIVE_REPOS, SILVER_PATH

    silver_path = silver_path or SILVER_PATH
    out_path = os.path.join(gold_path, "daily_active_repos") if gold_path else GOLD_DAILY_ACTIVE_REPOS

    logger.info("[Gold] daily_active_repos: reading from %s", silver_path)
    df = _read_silver(spark, silver_path)

    result = compute_daily_active_repos(df).orderBy("date", F.col("event_count").desc())

    _write_gold(result, out_path, partition_cols=["date"])
    return result


def compute_daily_active_repos(df: DataFrame) -> DataFrame:
    """Silver rows → one row per (date, repo_name)."""
    return (
        df.filter(F.col("repo_name").isNotNull() & F.col("date").isNotNull())
        .groupBy("date", "repo_name")
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("actor_login").alias("distinct_actors"),
        )
        .withColumn("last_updated_at", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# 2. PR merge time
# ---------------------------------------------------------------------------


def pr_merge_time(
    spark: SparkSession,
    silver_path: Optional[str] = None,
    gold_path: Optional[str] = None,
) -> DataFrame:
    """
    For merged pull requests, compute the time from PR creation to merge.

    Only PullRequestEvent rows where ``payload.pull_request.merged = true``
    are included.

    Schema
    ------
    event_id | date | repo_name | actor_login | pr_number | pr_title |
    pr_created_at | merged_at | hours_to_merge | last_updated_at
    """
    from conf.config import GOLD_PR_MERGE_TIME, SILVER_PATH

    silver_path = silver_path or SILVER_PATH
    out_path = os.path.join(gold_path, "pr_merge_time") if gold_path else GOLD_PR_MERGE_TIME

    logger.info("[Gold] pr_merge_time: reading from %s", silver_path)
    df = _read_silver(spark, silver_path)

    # Guard: payload struct must exist
    if "payload" not in df.columns:
        logger.warning("[Gold] pr_merge_time: 'payload' column not found — returning empty DF")
        return spark.createDataFrame([], schema=_PR_MERGE_TIME_EMPTY_SCHEMA(spark))

    result = compute_pr_merge_time(df).orderBy("date", "repo_name")

    _write_gold(result, out_path, partition_cols=["date"])
    return result


def compute_pr_merge_time(df: DataFrame) -> DataFrame:
    """Silver rows → one row per merged-PR event (keyed by ``event_id``)."""
    return (
        df.filter(
            (F.col("event_type") == "PullRequestEvent")
            & (F.col("payload.pull_request.merged") == True)  # noqa: E712
        )
        .select(
            F.col("id").alias("event_id"),
            F.col("date"),
            F.col("repo_name"),
            F.col("actor_login"),
            F.col("payload.number").alias("pr_number"),
            F.col("payload.pull_request.title").alias("pr_title"),
            F.col("payload.pull_request.created_at").alias("pr_created_at"),
            F.col("payload.pull_request.merged_at").alias("merged_at"),
        )
        .filter(F.col("pr_created_at").isNotNull() & F.col("merged_at").isNotNull())
        .withColumn(
            "hours_to_merge",
            (
                F.unix_timestamp("merged_at") - F.unix_timestamp("pr_created_at")
            ).cast("double")
            / 3600.0,
        )
        .withColumn("last_updated_at", F.current_timestamp())
    )


def _PR_MERGE_TIME_EMPTY_SCHEMA(spark: SparkSession) -> "pyspark.sql.types.StructType":  # noqa: F821
    from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                                   StructField, StructType, TimestampType,
                                   DateType)
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("date", DateType(), True),
        StructField("repo_name", StringType(), True),
        StructField("actor_login", StringType(), True),
        StructField("pr_number", IntegerType(), True),
        StructField("pr_title", StringType(), True),
        StructField("pr_created_at", TimestampType(), True),
        StructField("merged_at", TimestampType(), True),
        StructField("hours_to_merge", DoubleType(), True),
        StructField("last_updated_at", TimestampType(), True),
    ])


# ---------------------------------------------------------------------------
# 3. Top growing repos (WatchEvent / stars)
# ---------------------------------------------------------------------------


def top_growing_repos(
    spark: SparkSession,
    silver_path: Optional[str] = None,
    gold_path: Optional[str] = None,
) -> DataFrame:
    """
    Count WatchEvents (stars) per repo per ISO week and compute
    week-over-week growth rate.

    Schema
    ------
    year | week | repo_name | star_count | prev_star_count | wow_growth_rate |
    last_updated_at
    """
    from conf.config import GOLD_TOP_GROWING_REPOS, SILVER_PATH

    silver_path = silver_path or SILVER_PATH
    out_path = os.path.join(gold_path, "top_growing_repos") if gold_path else GOLD_TOP_GROWING_REPOS

    logger.info("[Gold] top_growing_repos: reading from %s", silver_path)
    df = _read_silver(spark, silver_path)

    result = compute_top_growing_repos(df).orderBy(F.col("wow_growth_rate").desc_nulls_last())

    _write_gold(result, out_path)
    return result


def compute_top_growing_repos(df: DataFrame) -> DataFrame:
    """
    Silver rows → one row per (repo_name, year, week).

    The lag window is per repo, so recomputing a repo needs *all* of that
    repo's WatchEvents — the incremental writer scopes by ``repo_name``.
    """
    weekly = (
        df.filter(
            (F.col("event_type") == "WatchEvent")
            & F.col("repo_name").isNotNull()
            & F.col("date").isNotNull()
        )
        .withColumn("year", F.year("date"))
        .withColumn("week", F.weekofyear("date"))
        .groupBy("year", "week", "repo_name")
        .agg(F.count("*").alias("star_count"))
    )

    # Window for previous week's star count (lag)
    w = Window.partitionBy("repo_name").orderBy("year", "week")
    return (
        weekly.withColumn("prev_star_count", F.lag("star_count", 1).over(w))
        .withColumn(
            "wow_growth_rate",
            F.when(
                F.col("prev_star_count").isNotNull() & (F.col("prev_star_count") > 0),
                (F.col("star_count") - F.col("prev_star_count")) / F.col("prev_star_count"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn("last_updated_at", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# 4. Contributor retention
# ---------------------------------------------------------------------------


def contributor_retention(
    spark: SparkSession,
    silver_path: Optional[str] = None,
    gold_path: Optional[str] = None,
) -> DataFrame:
    """
    For each actor, compute the number of distinct ISO weeks they were active
    and flag as ``retained`` if they were active in 2 or more consecutive weeks.

    Schema
    ------
    actor_login | active_weeks | max_consecutive_weeks | retained | last_updated_at
    """
    from conf.config import GOLD_CONTRIBUTOR_RETENTION, SILVER_PATH

    silver_path = silver_path or SILVER_PATH
    out_path = (
        os.path.join(gold_path, "contributor_retention")
        if gold_path
        else GOLD_CONTRIBUTOR_RETENTION
    )

    logger.info("[Gold] contributor_retention: reading from %s", silver_path)
    df = _read_silver(spark, silver_path)

    result = compute_contributor_retention(df).orderBy(F.col("active_weeks").desc())

    _write_gold(result, out_path)
    return result


def compute_contributor_retention(df: DataFrame) -> DataFrame:
    """Silver rows → one row per ``actor_login`` (scoped by actor)."""
    # Step 1: distinct (actor_login, year, week) pairs
    actor_weeks = (
        df.filter(F.col("actor_login").isNotNull() & F.col("date").isNotNull())
        .withColumn("year", F.year("date"))
        .withColumn("week", F.weekofyear("date"))
        .select("actor_login", "year", "week")
        .distinct()
        # Create a sortable week key: year * 100 + week
        .withColumn("week_key", F.col("year") * 100 + F.col("week"))
    )

    # Step 2: for each actor_login, use a window to detect consecutive weeks.
    # Two week_keys are consecutive if their difference is exactly 1 (within
    # the same year) or if next row is the first week of the next year.
    # We approximate with: consecutive if week_key difference == 1.
    w_actor = Window.partitionBy("actor_login").orderBy("week_key")
    actor_weeks = actor_weeks.withColumn(
        "prev_week_key", F.lag("week_key", 1).over(w_actor)
    ).withColumn(
        "is_consecutive",
        F.when(
            F.col("prev_week_key").isNotNull()
            & ((F.col("week_key") - F.col("prev_week_key")) == 1),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )

    # Step 3: aggregate per actor
    return (
        actor_weeks.groupBy("actor_login")
        .agg(
            F.countDistinct("week_key").alias("active_weeks"),
            F.sum("is_consecutive").alias("consecutive_week_pairs"),
        )
        .withColumn(
            "max_consecutive_weeks",
            # consecutive_week_pairs = N means N+1 consecutive weeks in a row
            F.col("consecutive_week_pairs") + 1,
        )
        .withColumn(
            "retained",
            F.col("consecutive_week_pairs") >= 1,  # at least 2 consecutive weeks
        )
        .drop("consecutive_week_pairs")
        .withColumn("last_updated_at", F.current_timestamp())
    )
