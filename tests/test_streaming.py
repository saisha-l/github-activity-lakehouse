"""
Tests for the incremental Structured Streaming pipeline and its exactly-once
recovery guarantees.

Crash simulation
----------------
A micro-batch is finished when Spark writes ``<checkpoint>/commits/<id>``.
Deleting the newest commit file reproduces a driver crash *after* the sink's
Delta commit but *before* the checkpoint commit: on restart Spark replays
that batch with identical input.  Idempotent MERGEs / txnVersion appends must
make the replay a no-op.

Test cases
----------
test_bronze_stream_only_reads_new_files
test_bronze_stream_dedups_across_files
test_bronze_replay_after_crash_is_idempotent
test_bronze_stream_ignores_in_flight_downloads
test_silver_replay_after_crash_is_idempotent
test_silver_has_change_data_feed
test_incremental_gold_matches_batch_rebuild
test_gold_replay_after_crash_is_idempotent
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

FIXTURE = Path(__file__).parent / "fixtures" / "sample_events.json"
GOLD_TABLES = {
    "daily_active_repos": ["date", "repo_name"],
    "pr_merge_time": ["event_id"],
    "top_growing_repos": ["repo_name", "year", "week"],
    "contributor_retention": ["actor_login"],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class Lake:
    """Temp-dir lakehouse layout for one test."""

    def __init__(self, root: Path) -> None:
        self.raw = root / "raw"
        self.raw.mkdir()
        self.bronze = str(root / "bronze")
        self.silver = str(root / "silver")
        self.gold = str(root / "gold")
        self.quarantine = str(root / "quarantine" / "bronze")
        self.checkpoints = str(root / "checkpoints")

    def add_file(self, name: str, lines: list[str]) -> None:
        # Hidden temp name + rename, exactly like download_gharchive.
        tmp = self.raw / f".{name}.part"
        tmp.write_text("\n".join(lines) + "\n")
        os.replace(tmp, self.raw / name)

    def checkpoint(self, stream: str) -> str:
        return os.path.join(self.checkpoints, stream)


def _fixture_lines() -> list[str]:
    return FIXTURE.read_text().strip().splitlines()


def _new_event(event_id: str, event_type: str, actor: str, repo: str, created_at: str) -> str:
    return json.dumps({
        "id": event_id,
        "type": event_type,
        "actor": {"id": 1, "login": actor},
        "repo": {"id": 1, "name": repo},
        "payload": {"action": "started"},
        "public": True,
        "created_at": created_at,
    })


def _run(spark: SparkSession, lake: Lake, **stages) -> dict:
    from src.pipeline import run_incremental_pipeline

    return run_incremental_pipeline(
        spark,
        raw_path=str(lake.raw),
        bronze_path=lake.bronze,
        silver_path=lake.silver,
        gold_path=lake.gold,
        checkpoint_root=lake.checkpoints,
        trigger_interval="",
        **stages,
    )


def _run_bronze(spark: SparkSession, lake: Lake) -> None:
    from src.bronze.ingest import start_bronze_stream

    start_bronze_stream(
        spark,
        raw_path=str(lake.raw),
        bronze_path=lake.bronze,
        checkpoint_path=lake.checkpoint("bronze"),
        quarantine_path=lake.quarantine,
        trigger_interval="",
        max_files_per_trigger=1,
    ).awaitTermination()


def _drop_last_commit(checkpoint: str) -> int:
    """Delete the newest ``commits/<id>`` file; return the batch id replayed."""
    commits = Path(checkpoint) / "commits"
    ids = sorted(int(p.name) for p in commits.iterdir() if p.name.isdigit())
    last = ids[-1]
    (commits / str(last)).unlink()
    crc = commits / f".{last}.crc"
    if crc.exists():
        crc.unlink()
    return last


def _read(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def _rows(df: DataFrame, keys: list[str]) -> list:
    """Comparable, order-independent table contents (minus audit timestamps)."""
    cols = sorted(c for c in df.columns if c != "last_updated_at")
    return sorted(df.select(*cols).collect(), key=lambda r: tuple(str(r[k]) for k in keys))


# ---------------------------------------------------------------------------
# Bronze
# ---------------------------------------------------------------------------


class TestBronzeStream:
    def test_bronze_stream_only_reads_new_files(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        """A second run must read only files added since the last checkpoint."""
        lake = Lake(tmp_path)
        lake.add_file("2024-01-15-0.json", _fixture_lines())
        stats1 = _run(spark, lake, silver=False, gold=False)
        assert stats1["bronze"].input_rows == 10
        assert _read(spark, lake.bronze).count() == 9

        new = [_new_event("35000000100", "WatchEvent", "zed", "zed/repo", "2024-01-16T09:00:00Z")]
        lake.add_file("2024-01-16-0.json", new)
        stats2 = _run(spark, lake, silver=False, gold=False)

        assert stats2["bronze"].input_rows == 1, "Already-checkpointed files must not be re-read"
        assert _read(spark, lake.bronze).count() == 10

    def test_bronze_stream_dedups_across_files(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        """An event repeated in a later file (GH Archive overlap) is inserted once."""
        lake = Lake(tmp_path)
        lines = _fixture_lines()
        lake.add_file("2024-01-15-0.json", lines[:5])
        lake.add_file("2024-01-15-1.json", lines[3:])  # overlaps ids 4 and 5
        _run_bronze(spark, lake)

        df = _read(spark, lake.bronze)
        assert df.count() == 9
        assert df.select("id").distinct().count() == 9

    def test_bronze_replay_after_crash_is_idempotent(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        """Replaying a committed micro-batch changes neither Bronze nor quarantine."""
        lake = Lake(tmp_path)
        lake.add_file("2024-01-15-0.json", _fixture_lines()[:5])
        lake.add_file("2024-01-15-1.json", _fixture_lines()[5:] + ['{"type": "PushEvent"}'])
        _run_bronze(spark, lake)

        bronze_before = _read(spark, lake.bronze).count()
        quarantine_before = _read(spark, lake.quarantine).count()
        assert quarantine_before == 1

        _drop_last_commit(lake.checkpoint("bronze"))
        _run_bronze(spark, lake)  # Spark replays the last batch

        assert _read(spark, lake.bronze).count() == bronze_before
        assert _read(spark, lake.quarantine).count() == quarantine_before


# ---------------------------------------------------------------------------
# Silver
# ---------------------------------------------------------------------------


class TestSilverStream:
    def test_silver_replay_after_crash_is_idempotent(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        lake = Lake(tmp_path)
        lake.add_file("2024-01-15-0.json", _fixture_lines())
        _run(spark, lake, gold=False)

        before = _rows(_read(spark, lake.silver).drop("_processed_at"), ["id"])
        assert len(before) == 9

        _drop_last_commit(lake.checkpoint("silver"))
        _run(spark, lake, bronze=False, gold=False)

        after = _rows(_read(spark, lake.silver).drop("_processed_at"), ["id"])
        assert after == before

    def test_silver_has_change_data_feed(self, spark: SparkSession, tmp_path: Path) -> None:
        lake = Lake(tmp_path)
        lake.add_file("2024-01-15-0.json", _fixture_lines())
        _run(spark, lake, gold=False)

        props = spark.sql(f"DESCRIBE DETAIL delta.`{lake.silver}`").first()["properties"]
        assert props.get("delta.enableChangeDataFeed") == "true"


# ---------------------------------------------------------------------------
# Gold
# ---------------------------------------------------------------------------


class TestGoldIncremental:
    def test_incremental_gold_matches_batch_rebuild(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        """
        Gold maintained incrementally over two runs must equal a full batch
        rebuild from the final Silver table.
        """
        from src.gold import aggregations as agg

        lake = Lake(tmp_path)
        lake.add_file("2024-01-15-0.json", _fixture_lines())
        _run(spark, lake)

        # Second increment touches existing keys (alice/awesome-project on
        # 2024-01-15, actor alice) and adds new ones.
        lake.add_file("2024-01-15-1.json", [
            _fixture_lines()[0],  # duplicate — must not double count
            _new_event("35000000100", "WatchEvent", "zed", "alice/awesome-project",
                       "2024-01-15T18:00:00Z"),
            _new_event("35000000101", "PushEvent", "alice", "alice/awesome-project",
                       "2024-01-29T09:00:00Z"),
        ])
        _run(spark, lake)

        silver = _read(spark, lake.silver)
        expected = {
            "daily_active_repos": agg.compute_daily_active_repos(silver),
            "pr_merge_time": agg.compute_pr_merge_time(silver),
            "top_growing_repos": agg.compute_top_growing_repos(silver),
            "contributor_retention": agg.compute_contributor_retention(silver),
        }
        for table, keys in GOLD_TABLES.items():
            actual = _read(spark, os.path.join(lake.gold, table))
            assert _rows(actual, keys) == _rows(expected[table], keys), table

        dar = _read(spark, os.path.join(lake.gold, "daily_active_repos"))
        row = dar.filter(
            (F.col("repo_name") == "alice/awesome-project") & (F.col("date") == "2024-01-15")
        ).first()
        assert row["event_count"] == 3  # push + 2 watches; the duplicate is ignored
        assert row["distinct_actors"] == 3

    def test_gold_replay_after_crash_is_idempotent(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        lake = Lake(tmp_path)
        lake.add_file("2024-01-15-0.json", _fixture_lines())
        _run(spark, lake)

        before = {
            t: _rows(_read(spark, os.path.join(lake.gold, t)), k) for t, k in GOLD_TABLES.items()
        }
        _drop_last_commit(lake.checkpoint("gold"))
        _run(spark, lake, bronze=False, silver=False)

        for table, keys in GOLD_TABLES.items():
            assert _rows(_read(spark, os.path.join(lake.gold, table)), keys) == before[table], table


@pytest.fixture(autouse=True)
def _clean_spark_streams(spark: SparkSession):
    yield
    for q in spark.streams.active:
        q.stop()


def test_bronze_stream_ignores_in_flight_downloads(spark: SparkSession, tmp_path: Path) -> None:
    """Hidden ``.part`` files (in-flight downloads) are invisible to the stream."""
    lake = Lake(tmp_path)
    (lake.raw / ".2024-01-15-0.json.gz.part").write_text(_fixture_lines()[0] + "\n")
    lake.add_file("2024-01-15-1.json", _fixture_lines()[1:3])
    _run_bronze(spark, lake)
    assert _read(spark, lake.bronze).count() == 2
