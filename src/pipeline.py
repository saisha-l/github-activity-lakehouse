"""
Incremental pipeline orchestration: raw files → Bronze → Silver → Gold.

Three Structured Streaming queries, each with its own checkpoint:

    raw/*.json.gz ──(file source)──▶ bronze_ingest    ──▶ bronze  (insert-only MERGE)
    bronze        ──(Delta source)─▶ silver_transform ──▶ silver  (upsert MERGE, CDF on)
    silver CDF    ──(Delta CDF)────▶ gold_incremental ──▶ gold/*  (scoped recompute MERGE)

Two run modes
-------------
* ``availableNow`` (no interval): run the stages in order, each draining its
  backlog in bounded micro-batches, then exit.  Schedule it (cron, Airflow,
  Databricks Jobs) for hourly / daily incremental loads.
* processing-time interval (e.g. ``"1 minute"``): start all three queries
  concurrently and run until one fails or the process is stopped.

Public API
----------
    run_incremental_pipeline(spark, ...) -> dict[str, StreamStats]
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

logger = logging.getLogger(__name__)


@dataclass
class StreamStats:
    batches: int = 0
    input_rows: int = 0


def _stats(query: StreamingQuery) -> StreamStats:
    progress = query.recentProgress
    batches = [p for p in progress if p.get("numInputRows", 0) > 0]
    return StreamStats(
        batches=len(batches),
        input_rows=sum(p["numInputRows"] for p in batches),
    )


def run_incremental_pipeline(
    spark: SparkSession,
    raw_path: Optional[str] = None,
    bronze_path: Optional[str] = None,
    silver_path: Optional[str] = None,
    gold_path: Optional[str] = None,
    checkpoint_root: Optional[str] = None,
    trigger_interval: Optional[str] = None,
    bronze: bool = True,
    silver: bool = True,
    gold: bool = True,
) -> Dict[str, StreamStats]:
    """
    Run the selected streaming stages and return per-stream statistics.

    ``checkpoint_root`` overrides the parent directory of the per-stream
    checkpoints (``<root>/bronze``, ``<root>/silver``, ``<root>/gold``).
    """
    from conf.config import CHECKPOINT_PATH, STREAM_TRIGGER_INTERVAL
    from src.bronze.ingest import start_bronze_stream
    from src.gold.incremental import start_gold_stream
    from src.silver.transform import start_silver_stream

    trigger_interval = trigger_interval if trigger_interval is not None else STREAM_TRIGGER_INTERVAL
    ckpt = checkpoint_root or CHECKPOINT_PATH

    starters: List[tuple[str, Callable[[], StreamingQuery]]] = []
    if bronze:
        starters.append(("bronze", lambda: start_bronze_stream(
            spark, raw_path=raw_path, bronze_path=bronze_path,
            checkpoint_path=os.path.join(ckpt, "bronze"),
            trigger_interval=trigger_interval,
        )))
    if silver:
        starters.append(("silver", lambda: start_silver_stream(
            spark, bronze_path=bronze_path, silver_path=silver_path,
            checkpoint_path=os.path.join(ckpt, "silver"),
            trigger_interval=trigger_interval,
        )))
    if gold:
        starters.append(("gold", lambda: start_gold_stream(
            spark, silver_path=silver_path, gold_path=gold_path,
            checkpoint_path=os.path.join(ckpt, "gold"),
            trigger_interval=trigger_interval,
        )))

    stats: Dict[str, StreamStats] = {}

    if not trigger_interval:
        # availableNow: each stage drains its backlog before the next starts,
        # so downstream stages see everything upstream just wrote.
        for name, start in starters:
            query = start()
            query.awaitTermination()
            stats[name] = _stats(query)
            logger.info("[%s] %d micro-batches, %d input rows",
                        name, stats[name].batches, stats[name].input_rows)
        return stats

    queries = {name: start() for name, start in starters}
    try:
        spark.streams.awaitAnyTermination()
    finally:
        for name, query in queries.items():
            if query.isActive:
                query.stop()
            stats[name] = _stats(query)
    for name, query in queries.items():
        if query.exception():
            raise RuntimeError(f"Stream '{name}' failed: {query.exception()}")
    return stats
