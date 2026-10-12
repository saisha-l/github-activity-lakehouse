"""
Silver layer — data quality checks.

Checks performed
----------------
1. null_rate       — fraction of rows where ``id`` is null (fail if > 5 %)
2. schema_drift    — compare actual columns to the expected canonical set
3. row_count       — total row count
4. duplicate_rate  — fraction of duplicate ``id`` values
5. quarantine      — bad rows (null id or null created_at) written to quarantine path

Public API
----------
    run_dq_checks(spark, silver_path) -> dict
    DataQualityError
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Custom exception
# ---------------------------------------------------------------------------


class DataQualityError(Exception):
    """Raised when a critical data-quality threshold is breached."""


# ---------------------------------------------------------------------------
# Expected schema (column names only — types are checked in schema.py)
# ---------------------------------------------------------------------------

EXPECTED_COLUMNS: List[str] = [
    "id",
    "type",
    "actor",
    "repo",
    "payload",
    "public",
    "created_at",
    "date",
    "repo_name",
    "actor_login",
    "event_type",
    "_processed_at",
]


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def run_dq_checks(
    spark: SparkSession,
    silver_path: Optional[str] = None,
    quarantine_path: Optional[str] = None,
    null_rate_threshold: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Run data quality checks on the Silver Delta table.

    Parameters
    ----------
    spark:
        Active SparkSession.
    silver_path:
        Path to the Silver Delta table.
    quarantine_path:
        Path where bad records are written.  Defaults to ``data/quarantine``.
    null_rate_threshold:
        Override the default null_rate threshold from config.

    Returns
    -------
    dict
        ``{
            "null_rate": float,
            "schema_drift": list[str],   # missing columns
            "row_count": int,
            "duplicate_rate": float,
            "quarantine_count": int,
            "passed": bool,
        }``

    Raises
    ------
    DataQualityError
        If ``null_rate`` exceeds the threshold.
    """
    from conf.config import DQ_NULL_RATE_THRESHOLD, QUARANTINE_PATH, SILVER_PATH

    silver_path = silver_path or SILVER_PATH
    quarantine_path = quarantine_path or QUARANTINE_PATH
    threshold = null_rate_threshold if null_rate_threshold is not None else DQ_NULL_RATE_THRESHOLD

    os.makedirs(quarantine_path, exist_ok=True)

    logger.info("[DQ] Loading Silver table from %s", silver_path)
    df = spark.read.format("delta").load(silver_path)

    total = df.count()
    results: Dict[str, Any] = {}

    # ------------------------------------------------------------------
    # 1. Row count
    # ------------------------------------------------------------------
    results["row_count"] = total
    logger.info("[DQ] row_count = %d", total)

    if total == 0:
        logger.warning("[DQ] Silver table is empty — skipping further checks.")
        results.update(
            {
                "null_rate": 0.0,
                "schema_drift": _schema_drift(df),
                "duplicate_rate": 0.0,
                "quarantine_count": 0,
                "passed": True,
            }
        )
        return results

    # ------------------------------------------------------------------
    # 2. Null rate on id
    # ------------------------------------------------------------------
    null_id_count = df.filter(F.col("id").isNull()).count()
    null_rate = null_id_count / total
    results["null_rate"] = null_rate
    logger.info("[DQ] null_rate (id) = %.4f", null_rate)

    # ------------------------------------------------------------------
    # 3. Schema drift
    # ------------------------------------------------------------------
    drift = _schema_drift(df)
    results["schema_drift"] = drift
    if drift:
        logger.warning("[DQ] Schema drift detected — missing columns: %s", drift)
    else:
        logger.info("[DQ] No schema drift detected.")

    # ------------------------------------------------------------------
    # 4. Duplicate rate
    # ------------------------------------------------------------------
    distinct_ids = df.filter(F.col("id").isNotNull()).select("id").distinct().count()
    non_null_total = total - null_id_count
    duplicate_rate = (
        (non_null_total - distinct_ids) / non_null_total if non_null_total > 0 else 0.0
    )
    results["duplicate_rate"] = duplicate_rate
    logger.info("[DQ] duplicate_rate = %.4f", duplicate_rate)

    # ------------------------------------------------------------------
    # 5. Quarantine bad records
    # ------------------------------------------------------------------
    bad_df = df.filter(F.col("id").isNull() | F.col("created_at").isNull())
    quarantine_count = bad_df.count()
    results["quarantine_count"] = quarantine_count

    if quarantine_count > 0:
        logger.warning("[DQ] Writing %d bad records to quarantine at %s",
                       quarantine_count, quarantine_path)
        (
            bad_df.withColumn("_quarantine_reason",
                              F.when(F.col("id").isNull(), F.lit("null_id"))
                              .when(F.col("created_at").isNull(), F.lit("null_created_at"))
                              .otherwise(F.lit("unknown")))
            .write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(quarantine_path)
        )
    else:
        logger.info("[DQ] No bad records found.")

    # ------------------------------------------------------------------
    # 6. Overall pass/fail
    # ------------------------------------------------------------------
    passed = null_rate <= threshold
    results["passed"] = passed

    if not passed:
        msg = (
            f"Data quality check FAILED: null_rate={null_rate:.4f} "
            f"exceeds threshold={threshold:.4f}"
        )
        logger.error(msg)
        raise DataQualityError(msg)

    logger.info("[DQ] All checks passed.")
    return results


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _schema_drift(df) -> List[str]:
    """Return expected columns that are absent from the DataFrame."""
    actual = set(df.columns)
    return [c for c in EXPECTED_COLUMNS if c not in actual]
