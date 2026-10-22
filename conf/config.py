"""
Central configuration for the GitHub Activity Lakehouse pipeline.
All paths are relative to the project root; override via environment variables
for cluster deployments.
"""

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Root resolution
# ---------------------------------------------------------------------------
# Allow callers to set LAKEHOUSE_ROOT to point to a remote path (e.g. dbfs:/)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
LAKEHOUSE_ROOT = os.environ.get("LAKEHOUSE_ROOT", str(_PROJECT_ROOT / "data"))

# ---------------------------------------------------------------------------
# Layer paths
# ---------------------------------------------------------------------------
RAW_PATH = os.path.join(LAKEHOUSE_ROOT, "raw")
BRONZE_PATH = os.path.join(LAKEHOUSE_ROOT, "bronze")
SILVER_PATH = os.path.join(LAKEHOUSE_ROOT, "silver")
GOLD_PATH = os.path.join(LAKEHOUSE_ROOT, "gold")
CHECKPOINT_PATH = os.path.join(LAKEHOUSE_ROOT, "checkpoints")
QUARANTINE_PATH = os.path.join(LAKEHOUSE_ROOT, "quarantine")

# ---------------------------------------------------------------------------
# Ingestion settings
# ---------------------------------------------------------------------------
PARTITION_COL = "date"
DEDUP_KEY = "id"
# Raw GH Archive files per Bronze micro-batch (24 = one day of hourly files).
MAX_FILES_PER_TRIGGER = int(os.environ.get("MAX_FILES_PER_TRIGGER", "24"))
# Delta files per Silver / Gold micro-batch.
DELTA_MAX_FILES_PER_TRIGGER = int(os.environ.get("DELTA_MAX_FILES_PER_TRIGGER", "200"))
# Bronze files per Silver micro-batch.  Each Bronze micro-batch lands as one
# large file per date (~0.5-1 GB, ~600k events), and parsing a whole
# multi-day backlog of raw JSON in one batch runs the driver out of heap, so
# Silver takes a few files at a time.
SILVER_MAX_FILES_PER_TRIGGER = int(os.environ.get("SILVER_MAX_FILES_PER_TRIGGER", "3"))
# What the Bronze file stream does with a raw file once its micro-batch is
# committed: "off" (keep), "delete", or "archive" (move to RAW_ARCHIVE_PATH).
# "delete" keeps disk usage flat during large backfills — a file is only
# removed after the checkpoint records it, so recovery is unaffected.
RAW_CLEAN_SOURCE = os.environ.get("RAW_CLEAN_SOURCE", "off")
RAW_ARCHIVE_PATH = os.environ.get("RAW_ARCHIVE_PATH", os.path.join(LAKEHOUSE_ROOT, "raw_archive"))

# ---------------------------------------------------------------------------
# Streaming
# ---------------------------------------------------------------------------
# Empty  -> Trigger.AvailableNow (drain everything available, then stop).
# "1 minute" etc. -> long-running processingTime trigger.
STREAM_TRIGGER_INTERVAL = os.environ.get("STREAM_TRIGGER_INTERVAL", "")

BRONZE_CHECKPOINT = os.path.join(CHECKPOINT_PATH, "bronze")
SILVER_CHECKPOINT = os.path.join(CHECKPOINT_PATH, "silver")
GOLD_CHECKPOINT = os.path.join(CHECKPOINT_PATH, "gold")
BRONZE_QUARANTINE_PATH = os.path.join(QUARANTINE_PATH, "bronze")

# ---------------------------------------------------------------------------
# GH Archive source
# ---------------------------------------------------------------------------
GHARCHIVE_BASE_URL = "https://data.gharchive.org"

# ---------------------------------------------------------------------------
# Spark tuning knobs  (can be overridden at runtime)
# ---------------------------------------------------------------------------
SPARK_SHUFFLE_PARTITIONS = int(os.environ.get("SPARK_SHUFFLE_PARTITIONS", "200"))
SPARK_DEFAULT_PARALLELISM = int(os.environ.get("SPARK_DEFAULT_PARALLELISM", "8"))
# Local mode runs everything in the driver JVM, so this is the job's whole
# heap.  Spark's 1g default is far too small for day-sized micro-batches.
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "4g")
# Bronze stores every event's raw JSON; zstd shrinks it far more than Spark's
# default snappy, which decides whether a month-scale backfill fits on disk.
SPARK_PARQUET_CODEC = os.environ.get("SPARK_PARQUET_CODEC", "zstd")
# Parquet row-group size.  A reader holds a whole decompressed row group in
# memory, and Bronze's raw JSON expands ~8x, so the 128 MB default means ~1 GB
# of heap per concurrent task.  32 MB keeps that ~250 MB.
SPARK_PARQUET_BLOCK_SIZE_MB = int(os.environ.get("SPARK_PARQUET_BLOCK_SIZE_MB", "32"))
# Cores for local mode ("*" = all).  Lower it to cut peak memory.
SPARK_LOCAL_CORES = os.environ.get("SPARK_LOCAL_CORES", "*")

# ---------------------------------------------------------------------------
# Data-quality thresholds
# ---------------------------------------------------------------------------
DQ_NULL_RATE_THRESHOLD = 0.05  # fail if > 5 % of ids are null

# ---------------------------------------------------------------------------
# Gold sub-paths
# ---------------------------------------------------------------------------
GOLD_DAILY_ACTIVE_REPOS = os.path.join(GOLD_PATH, "daily_active_repos")
GOLD_PR_MERGE_TIME = os.path.join(GOLD_PATH, "pr_merge_time")
GOLD_TOP_GROWING_REPOS = os.path.join(GOLD_PATH, "top_growing_repos")
GOLD_CONTRIBUTOR_RETENTION = os.path.join(GOLD_PATH, "contributor_retention")
