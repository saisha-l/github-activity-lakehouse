"""
Tests for the Gold aggregation layer (src/gold/aggregations.py).

Test cases
----------
test_daily_active_repos_row_count
    daily_active_repos should produce one row per (date, repo_name) combination.

test_daily_active_repos_columns
    Result must have: date, repo_name, event_count, distinct_actors.

test_pr_merge_time_only_merged_prs
    pr_merge_time must contain only rows where the PR was merged.

test_pr_merge_time_hours_positive
    hours_to_merge must be >= 0 for all merged PRs.

test_top_growing_repos_columns
    Result must have: year, week, repo_name, star_count.

test_contributor_retention_identifies_retained
    An actor active in consecutive weeks must be flagged as retained=True.

test_contributor_retention_not_retained_single_week
    An actor active in only one week must be flagged as retained=False.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DateType, IntegerType, LongType,
                                StringType, StructField, StructType,
                                TimestampType)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FIXTURE = Path(__file__).parent / "fixtures" / "sample_events.json"


def _build_silver(spark: SparkSession, tmp_path: Path) -> str:
    """Build a Silver Delta table from the fixture and return its path."""
    from src.bronze.ingest import _ingest_batch
    from src.silver.transform import bronze_to_silver

    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(exist_ok=True)
    shutil.copy(str(FIXTURE), str(raw_dir / "sample_events.json"))

    bronze_path = str(tmp_path / "bronze")
    silver_path = str(tmp_path / "silver")

    _ingest_batch(spark, str(raw_dir), bronze_path, ingested_at="2024-01-15T10:00:00+00:00")
    bronze_to_silver(spark, bronze_path=bronze_path, silver_path=silver_path)
    return silver_path


def _make_retention_silver(spark: SparkSession, tmp_path: Path) -> str:
    """
    Build a minimal Silver table with known actor / date combinations for
    testing contributor retention logic.

    actor_a — active weeks 1 and 2 (consecutive) → retained
    actor_b — active week 1 only               → not retained
    actor_c — active weeks 1, 3 (gap)          → not retained
    """
    silver_path = str(tmp_path / "silver_retention")

    rows = [
        # actor_a: week 3 (2024-01-15) and week 4 (2024-01-22) — consecutive
        ("a001", "PushEvent", "actor_a", "repo_x", "2024-01-15"),
        ("a002", "PushEvent", "actor_a", "repo_x", "2024-01-22"),
        # actor_b: week 3 only
        ("b001", "WatchEvent", "actor_b", "repo_x", "2024-01-15"),
        # actor_c: week 3 and week 5 (gap)
        ("c001", "PushEvent", "actor_c", "repo_y", "2024-01-15"),
        ("c002", "PushEvent", "actor_c", "repo_y", "2024-02-05"),
    ]

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("actor_login", StringType(), True),
        StructField("repo_name", StringType(), True),
        StructField("_date_str", StringType(), True),
    ])

    df = (
        spark.createDataFrame(rows, schema)
        .withColumn("date", F.to_date(F.col("_date_str")))
        .withColumn("created_at", F.to_timestamp(F.col("_date_str")))
        .withColumn("_processed_at", F.current_timestamp())
        .drop("_date_str")
    )

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(silver_path)
    )
    return silver_path


# ---------------------------------------------------------------------------
# daily_active_repos
# ---------------------------------------------------------------------------


class TestDailyActiveRepos:
    def test_row_count(self, spark: SparkSession, tmp_path: Path) -> None:
        """One row per (date, repo_name) combination."""
        from src.gold.aggregations import daily_active_repos

        silver_path = _build_silver(spark, tmp_path)
        gold_path = str(tmp_path / "gold")

        result = daily_active_repos(spark, silver_path=silver_path, gold_path=gold_path)

        # Our fixture has events across 4 distinct repos on 2 distinct dates.
        # The exact count depends on which repos have events on which dates.
        assert result.count() > 0, "daily_active_repos should return at least one row"

        # Verify no duplicate (date, repo_name) pairs
        total = result.count()
        distinct = result.select("date", "repo_name").distinct().count()
        assert total == distinct, "Each (date, repo_name) pair should appear exactly once"

    def test_columns(self, spark: SparkSession, tmp_path: Path) -> None:
        """Result must have required columns."""
        from src.gold.aggregations import daily_active_repos

        silver_path = _build_silver(spark, tmp_path)
        gold_path = str(tmp_path / "gold")

        result = daily_active_repos(spark, silver_path=silver_path, gold_path=gold_path)

        for col in ("date", "repo_name", "event_count", "distinct_actors"):
            assert col in result.columns, f"Column '{col}' missing from daily_active_repos"

    def test_event_count_non_zero(self, spark: SparkSession, tmp_path: Path) -> None:
        """event_count must be > 0 for every row."""
        from src.gold.aggregations import daily_active_repos

        silver_path = _build_silver(spark, tmp_path)
        gold_path = str(tmp_path / "gold")

        result = daily_active_repos(spark, silver_path=silver_path, gold_path=gold_path)
        zero_count_rows = result.filter(F.col("event_count") <= 0).count()
        assert zero_count_rows == 0, "All event_count values must be > 0"


# ---------------------------------------------------------------------------
# pr_merge_time
# ---------------------------------------------------------------------------


class TestPrMergeTime:
    def test_only_merged_prs(self, spark: SparkSession, tmp_path: Path) -> None:
        """pr_merge_time must only include rows from merged PRs."""
        from src.gold.aggregations import pr_merge_time

        silver_path = _build_silver(spark, tmp_path)
        gold_path = str(tmp_path / "gold")

        result = pr_merge_time(spark, silver_path=silver_path, gold_path=gold_path)

        # Our fixture has exactly 1 merged PR (event id 35000000004)
        assert result.count() == 1, (
            f"Expected 1 merged PR row, got {result.count()}"
        )

    def test_hours_to_merge_positive(self, spark: SparkSession, tmp_path: Path) -> None:
        """hours_to_merge must be >= 0."""
        from src.gold.aggregations import pr_merge_time

        silver_path = _build_silver(spark, tmp_path)
        gold_path = str(tmp_path / "gold")

        result = pr_merge_time(spark, silver_path=silver_path, gold_path=gold_path)
        negative_rows = result.filter(F.col("hours_to_merge") < 0).count()
        assert negative_rows == 0, "hours_to_merge must be non-negative"

    def test_merged_pr_repo_name(self, spark: SparkSession, tmp_path: Path) -> None:
        """The merged PR should have the correct repo name from the fixture."""
        from src.gold.aggregations import pr_merge_time

        silver_path = _build_silver(spark, tmp_path)
        gold_path = str(tmp_path / "gold")

        result = pr_merge_time(spark, silver_path=silver_path, gold_path=gold_path)
        row = result.first()
        assert row is not None
        assert row["repo_name"] == "dave/ml-toolkit", (
            f"Expected repo 'dave/ml-toolkit', got '{row['repo_name']}'"
        )


# ---------------------------------------------------------------------------
# top_growing_repos
# ---------------------------------------------------------------------------


class TestTopGrowingRepos:
    def test_columns(self, spark: SparkSession, tmp_path: Path) -> None:
        """Result must include year, week, repo_name, star_count."""
        from src.gold.aggregations import top_growing_repos

        silver_path = _build_silver(spark, tmp_path)
        gold_path = str(tmp_path / "gold")

        result = top_growing_repos(spark, silver_path=silver_path, gold_path=gold_path)
        for col in ("year", "week", "repo_name", "star_count"):
            assert col in result.columns, f"Column '{col}' missing from top_growing_repos"

    def test_only_watch_events(self, spark: SparkSession, tmp_path: Path) -> None:
        """Only WatchEvent rows contribute to top_growing_repos."""
        from src.gold.aggregations import top_growing_repos

        silver_path = _build_silver(spark, tmp_path)
        gold_path = str(tmp_path / "gold")

        result = top_growing_repos(spark, silver_path=silver_path, gold_path=gold_path)

        # Our fixture has 2 WatchEvents on 2 different repos
        assert result.count() == 2, (
            f"Expected 2 WatchEvent rows, got {result.count()}"
        )

    def test_star_count_positive(self, spark: SparkSession, tmp_path: Path) -> None:
        """star_count must be > 0."""
        from src.gold.aggregations import top_growing_repos

        silver_path = _build_silver(spark, tmp_path)
        gold_path = str(tmp_path / "gold")

        result = top_growing_repos(spark, silver_path=silver_path, gold_path=gold_path)
        zero_rows = result.filter(F.col("star_count") <= 0).count()
        assert zero_rows == 0


# ---------------------------------------------------------------------------
# contributor_retention
# ---------------------------------------------------------------------------


class TestContributorRetention:
    def test_retained_actor_identified(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        """actor_a is active on consecutive weeks — must be retained=True."""
        from src.gold.aggregations import contributor_retention

        silver_path = _make_retention_silver(spark, tmp_path)
        gold_path = str(tmp_path / "gold")

        result = contributor_retention(spark, silver_path=silver_path, gold_path=gold_path)

        actor_a = result.filter(F.col("actor_login") == "actor_a").first()
        assert actor_a is not None, "actor_a should be in contributor_retention"
        assert actor_a["retained"] is True, (
            "actor_a (consecutive weeks) must be flagged as retained"
        )

    def test_single_week_not_retained(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        """actor_b is active only one week — must be retained=False."""
        from src.gold.aggregations import contributor_retention

        silver_path = _make_retention_silver(spark, tmp_path)
        gold_path = str(tmp_path / "gold")

        result = contributor_retention(spark, silver_path=silver_path, gold_path=gold_path)

        actor_b = result.filter(F.col("actor_login") == "actor_b").first()
        assert actor_b is not None, "actor_b should be in contributor_retention"
        assert actor_b["retained"] is False, (
            "actor_b (single week) must not be flagged as retained"
        )

    def test_gap_week_not_retained(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        """actor_c has a gap between weeks — must be retained=False."""
        from src.gold.aggregations import contributor_retention

        silver_path = _make_retention_silver(spark, tmp_path)
        gold_path = str(tmp_path / "gold")

        result = contributor_retention(spark, silver_path=silver_path, gold_path=gold_path)

        actor_c = result.filter(F.col("actor_login") == "actor_c").first()
        assert actor_c is not None, "actor_c should be in contributor_retention"
        assert actor_c["retained"] is False, (
            "actor_c (gap between weeks) must not be flagged as retained"
        )

    def test_columns(self, spark: SparkSession, tmp_path: Path) -> None:
        """Result must have required columns."""
        from src.gold.aggregations import contributor_retention

        silver_path = _make_retention_silver(spark, tmp_path)
        gold_path = str(tmp_path / "gold")

        result = contributor_retention(spark, silver_path=silver_path, gold_path=gold_path)
        for col in ("actor_login", "active_weeks", "max_consecutive_weeks", "retained"):
            assert col in result.columns, f"Column '{col}' missing from contributor_retention"
