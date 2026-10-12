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
MAX_FILES_PER_TRIGGER = 10

# ---------------------------------------------------------------------------
# GH Archive source
# ---------------------------------------------------------------------------
GHARCHIVE_BASE_URL = "https://data.gharchive.org"

# ---------------------------------------------------------------------------
# Spark tuning knobs  (can be overridden at runtime)
# ---------------------------------------------------------------------------
SPARK_SHUFFLE_PARTITIONS = int(os.environ.get("SPARK_SHUFFLE_PARTITIONS", "200"))
SPARK_DEFAULT_PARALLELISM = int(os.environ.get("SPARK_DEFAULT_PARALLELISM", "8"))

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
