#!/usr/bin/env python3
"""
Run the full GitHub Activity Lakehouse pipeline or individual stages.

Usage
-----
    # Incremental (Structured Streaming, checkpointed, exactly-once):
    python scripts/run_pipeline.py --streaming --bronze --silver --gold
    python scripts/run_pipeline.py --streaming --bronze --silver --gold \
        --trigger-interval "1 minute"          # long-running

    # Batch full rebuild / repair (idempotent MERGE / overwrite):
    python scripts/run_pipeline.py --bronze --silver --dq --gold

Options
-------
--bronze        Run the Bronze ingestion stage
--silver        Run the Silver transformation stage
--gold          Run all Gold aggregation stages
--dq            Run Silver data-quality checks (after Silver)
--streaming     Run the selected stages as checkpointed Structured Streaming
                queries (Bronze file stream, Silver Delta stream, Gold CDF
                stream) instead of batch full reprocessing
--trigger-interval  Processing-time trigger, e.g. "1 minute" (streaming only;
                default: availableNow — drain the backlog, then exit)
--checkpoint-path   Override the checkpoint root directory
--stats         Print row counts and Delta versions of each table at the end
--raw-path      Override raw data path
--bronze-path   Override Bronze Delta table path
--silver-path   Override Silver Delta table path
--gold-path     Override Gold Delta table path
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Optional

# Ensure project root is on path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("run_pipeline")


# ---------------------------------------------------------------------------
# Timing helper
# ---------------------------------------------------------------------------


class _Timer:
    def __init__(self, name: str) -> None:
        self.name = name
        self._start: Optional[float] = None

    def __enter__(self) -> "_Timer":
        self._start = time.perf_counter()
        logger.info(">>> Starting stage: %s", self.name)
        return self

    def __exit__(self, *_) -> None:
        elapsed = time.perf_counter() - self._start  # type: ignore[operator]
        logger.info("<<< Finished stage: %s  (%.2f s)", self.name, elapsed)
        print(f"  {self.name:<30} {elapsed:8.2f} s")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="GitHub Activity Lakehouse pipeline runner.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--bronze", action="store_true", help="Run Bronze ingestion")
    parser.add_argument("--silver", action="store_true", help="Run Silver transform")
    parser.add_argument("--gold", action="store_true", help="Run Gold aggregations")
    parser.add_argument("--dq", action="store_true", help="Run DQ checks on Silver")
    parser.add_argument(
        "--streaming",
        action="store_true",
        help="Incremental Structured Streaming mode (default: batch rebuild)",
    )
    parser.add_argument(
        "--trigger-interval",
        default=None,
        metavar="INTERVAL",
        help='Processing-time trigger, e.g. "1 minute" (default: availableNow)',
    )
    parser.add_argument("--checkpoint-path", default=None)
    parser.add_argument("--stats", action="store_true", help="Print table stats at the end")
    parser.add_argument("--raw-path", default=None)
    parser.add_argument("--bronze-path", default=None)
    parser.add_argument("--silver-path", default=None)
    parser.add_argument("--gold-path", default=None)

    args = parser.parse_args()
    if not any([args.bronze, args.silver, args.gold, args.dq, args.stats]):
        parser.error("At least one of --bronze, --silver, --gold, --dq, --stats must be specified.")
    if args.trigger_interval and not args.streaming:
        parser.error("--trigger-interval requires --streaming.")
    return args


# ---------------------------------------------------------------------------
# Stage runners
# ---------------------------------------------------------------------------


def _run_streaming(spark, args, raw_path, bronze_path, silver_path, gold_path, checkpoint_path):
    from src.pipeline import run_incremental_pipeline

    with _Timer("Incremental streaming"):
        stats = run_incremental_pipeline(
            spark,
            raw_path=raw_path,
            bronze_path=bronze_path,
            silver_path=silver_path,
            gold_path=gold_path,
            checkpoint_root=checkpoint_path,
            trigger_interval=args.trigger_interval,
            bronze=args.bronze,
            silver=args.silver,
            gold=args.gold,
        )
    print("\n  Stream progress this run:")
    for name, st in stats.items():
        print(f"    {name:<10} {st.batches:>5} micro-batches  {st.input_rows:>14,} input rows")


def _run_batch(spark, args, raw_path, bronze_path, silver_path, gold_path):
    if args.bronze:
        with _Timer("Bronze ingestion"):
            from src.bronze.ingest import ingest_to_bronze

            ingest_to_bronze(
                spark,
                raw_path=raw_path,
                bronze_path=bronze_path,
                use_streaming=False,
            )

    if args.silver:
        with _Timer("Silver transform"):
            from src.silver.transform import bronze_to_silver

            bronze_to_silver(spark, bronze_path=bronze_path, silver_path=silver_path)

    if args.dq:
        _run_dq(spark, silver_path)

    if args.gold:
        from src.gold.aggregations import (contributor_retention,
                                           daily_active_repos, pr_merge_time,
                                           top_growing_repos)

        with _Timer("Gold: daily_active_repos"):
            daily_active_repos(spark, silver_path=silver_path, gold_path=gold_path)

        with _Timer("Gold: pr_merge_time"):
            pr_merge_time(spark, silver_path=silver_path, gold_path=gold_path)

        with _Timer("Gold: top_growing_repos"):
            top_growing_repos(spark, silver_path=silver_path, gold_path=gold_path)

        with _Timer("Gold: contributor_retention"):
            contributor_retention(spark, silver_path=silver_path, gold_path=gold_path)


def _run_dq(spark, silver_path):
    with _Timer("Data quality checks"):
        from src.silver.dq import run_dq_checks

        results = run_dq_checks(spark, silver_path=silver_path)
        print("\n  DQ Results:")
        for k, v in results.items():
            print(f"    {k:<25} {v}")


def _print_stats(spark, bronze_path, silver_path, gold_path):
    import os

    from delta.tables import DeltaTable

    tables = [("bronze", bronze_path), ("silver", silver_path)] + [
        (f"gold/{t}", os.path.join(gold_path, t))
        for t in ("daily_active_repos", "pr_merge_time", "top_growing_repos",
                  "contributor_retention")
    ]
    print("\n  Table stats:")
    for name, path in tables:
        if not DeltaTable.isDeltaTable(spark, path):
            print(f"    {name:<28} (missing)")
            continue
        version = DeltaTable.forPath(spark, path).history(1).first()["version"]
        rows = spark.read.format("delta").load(path).count()
        print(f"    {name:<28} {rows:>14,} rows   v{version}")


def main() -> None:
    args = _parse_args()

    from conf.config import (BRONZE_PATH, CHECKPOINT_PATH, GOLD_PATH,
                             RAW_PATH, SILVER_PATH)
    from src.utils.spark_session import get_spark

    raw_path = args.raw_path or RAW_PATH
    bronze_path = args.bronze_path or BRONZE_PATH
    silver_path = args.silver_path or SILVER_PATH
    gold_path = args.gold_path or GOLD_PATH
    checkpoint_path = args.checkpoint_path or CHECKPOINT_PATH

    print("\n" + "=" * 60)
    print("  GitHub Activity Lakehouse — Pipeline Run")
    print("=" * 60)
    print(f"  Mode        : {'streaming (' + (args.trigger_interval or 'availableNow') + ')' if args.streaming else 'batch'}")
    print(f"  Raw path    : {raw_path}")
    print(f"  Bronze path : {bronze_path}")
    print(f"  Silver path : {silver_path}")
    print(f"  Gold path   : {gold_path}")
    print(f"  Checkpoints : {checkpoint_path}")
    print("=" * 60 + "\n")
    print(f"  {'Stage':<30} {'Elapsed':>8}")
    print("  " + "-" * 40)

    spark = get_spark("GitHubLakehouse-Run")

    if args.streaming:
        _run_streaming(spark, args, raw_path, bronze_path, silver_path, gold_path, checkpoint_path)
        if args.dq:
            _run_dq(spark, silver_path)
    else:
        _run_batch(spark, args, raw_path, bronze_path, silver_path, gold_path)

    if args.stats:
        _print_stats(spark, bronze_path, silver_path, gold_path)

    print("\n" + "=" * 60)
    print("  Pipeline complete.")
    print("=" * 60 + "\n")

    spark.stop()


if __name__ == "__main__":
    main()
