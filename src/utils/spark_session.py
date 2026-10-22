"""
Centralised SparkSession factory.

Usage
-----
    from src.utils.spark_session import get_spark
    spark = get_spark("my-app")          # local mode
    spark = get_spark("my-app", local=False)  # cluster / Databricks
"""

from __future__ import annotations

import os
import sys
from typing import Optional

from pyspark.sql import SparkSession


def get_spark(
    app_name: str = "GitHubActivityLakehouse",
    local: bool = True,
    shuffle_partitions: Optional[int] = None,
) -> SparkSession:
    """
    Build (or reuse) a SparkSession with Delta Lake extensions configured.

    Parameters
    ----------
    app_name:
        Spark application name shown in the UI / logs.
    local:
        When True the session runs in local[*] mode (all available cores).
        Set to False when running on a real cluster or Databricks.
    shuffle_partitions:
        Override the default shuffle partition count.  If None the value from
        ``conf/config.py`` is used.

    Returns
    -------
    SparkSession
    """
    from conf.config import (SPARK_PARQUET_BLOCK_SIZE_MB, SPARK_PARQUET_CODEC,
                             SPARK_SHUFFLE_PARTITIONS)

    n_partitions = shuffle_partitions or SPARK_SHUFFLE_PARTITIONS

    builder = (
        SparkSession.builder.appName(app_name)
        # --- Delta Lake extensions ---
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # --- Performance tuning ---
        .config("spark.sql.shuffle.partitions", str(n_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.parquet.compression.codec", SPARK_PARQUET_CODEC)
        .config("spark.hadoop.parquet.block.size", str(SPARK_PARQUET_BLOCK_SIZE_MB * 1024 * 1024))
        # --- Delta-specific ---
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.delta.logStore.class",
                "org.apache.spark.sql.delta.storage.HDFSLogStore")
        # --- Merge schema support ---
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        # --- Structured Streaming ---
        # Keep enough progress history to report totals for long backfills.
        .config("spark.sql.streaming.numRecentProgressUpdates", "1000")
        # Shuffle files are only removed once the JVM garbage-collects their
        # ShuffleDependency.  Spark forces a GC every 30 min by default, so a
        # long backfill would pile up tens of GB of finished micro-batches'
        # shuffle data on local disk.
        .config("spark.cleaner.periodicGC.interval", "2min")
    )

    if local:
        from conf.config import SPARK_DRIVER_MEMORY, SPARK_LOCAL_CORES

        builder = builder.master(f"local[{SPARK_LOCAL_CORES}]").config(
            "spark.driver.memory", SPARK_DRIVER_MEMORY
        )
        # Python workers must use the driver's interpreter (e.g. the venv).
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    # Allow the Delta JAR to be picked up from the environment when running
    # locally.  On Databricks / EMR the runtime already bundles Delta.
    delta_jar = os.environ.get("DELTA_JAR", "")
    if delta_jar:
        builder = builder.config("spark.jars", delta_jar)
    elif local:
        # pip-installed delta-spark: add the matching Delta jars to the session.
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
