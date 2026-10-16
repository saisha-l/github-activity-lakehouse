"""
Shared helpers for the incremental (Structured Streaming) pipeline.

Exactly-once model
------------------
Every stream in this project writes through ``foreachBatch`` and a checkpoint:

* The checkpoint records which source offsets belong to each micro-batch
  (``offsets/<id>``) and which batches finished (``commits/<id>``).
* If the driver dies after a batch's Delta commit but before the checkpoint
  commit, Spark replays the *same* batch id with the *same* input on restart.
* Every sink write is therefore idempotent — MERGE on a natural key, or a
  Delta append tagged with ``txnAppId``/``txnVersion`` — so a replay changes
  nothing.  At-least-once delivery + idempotent writes = exactly-once results.

Public API
----------
    build_trigger(interval)               -> kwargs for DataStreamWriter.trigger
    ensure_delta_table(spark, path, ...)  -> create an empty table if missing
    partition_values(df, col)             -> distinct partition values in a batch
    idempotent_append(df, path, app_id, batch_id)
    merge_insert_only(spark, df, path, keys, prune_col)
    merge_upsert(spark, df, path, keys, prune_col, update_condition)
    replace_scope(spark, recomputed, path, scope_cols, merge_keys, ...)
"""

from __future__ import annotations

import logging
from typing import Dict, Iterable, List, Optional, Sequence

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Triggers / table bootstrap
# ---------------------------------------------------------------------------


def build_trigger(interval: Optional[str] = None) -> Dict[str, object]:
    """
    Return kwargs for ``DataStreamWriter.trigger``.

    An empty interval means ``availableNow`` — process every file/commit that
    exists right now (in bounded micro-batches) and stop.  Re-running later
    picks up exactly where the checkpoint left off, which is what makes the
    pipeline incremental when scheduled hourly/daily.
    """
    if interval:
        return {"processingTime": interval}
    return {"availableNow": True}


def ensure_delta_table(
    spark: SparkSession,
    path: str,
    schema: StructType,
    partition_cols: Sequence[str] = (),
    properties: Optional[Dict[str, str]] = None,
) -> None:
    """
    Create an empty Delta table at *path* if one does not exist yet.

    Creating tables up front lets downstream streams start before the first
    upstream micro-batch lands, and pins the schema/partitioning instead of
    leaving them to whichever batch happens to write first.
    """
    properties = properties or {}
    if DeltaTable.isDeltaTable(spark, path):
        # Make sure required properties (e.g. Change Data Feed) are set on
        # tables created by older versions of the pipeline.
        current = DeltaTable.forPath(spark, path).detail().first()["properties"] or {}
        missing = {k: v for k, v in properties.items() if current.get(k) != v}
        if missing:
            props = ", ".join(f"'{k}' = '{v}'" for k, v in missing.items())
            spark.sql(f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES ({props})")
            logger.info("Set table properties on %s: %s", path, missing)
        return

    builder = DeltaTable.createIfNotExists(spark).location(path).addColumns(schema)
    if partition_cols:
        builder = builder.partitionedBy(*partition_cols)
    for k, v in properties.items():
        builder = builder.property(k, v)
    builder.execute()
    logger.info("Created Delta table at %s", path)


def partition_values(df: DataFrame, col: str) -> List[object]:
    """Distinct non-null values of *col* in a (small, persisted) batch."""
    return [r[0] for r in df.select(col).where(F.col(col).isNotNull()).distinct().collect()]


def _prune_predicate(alias: str, col: str, values: Iterable[object]) -> Optional[str]:
    """
    Literal ``IN`` predicate on the target's partition column.

    Delta can only prune target files on literal predicates in the MERGE
    condition, so without this every micro-batch would scan the whole
    (100M+ row) target table looking for key matches.
    """
    values = list(values)
    if not values:
        return None
    lits = ", ".join(f"'{v}'" for v in values)
    return f"{alias}.{col} IN ({lits})"


def _merge_condition(keys: Sequence[str], prune: Optional[str]) -> str:
    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    return f"{prune} AND {cond}" if prune else cond


# ---------------------------------------------------------------------------
# Idempotent writers
# ---------------------------------------------------------------------------


def idempotent_append(
    df: DataFrame,
    path: str,
    app_id: str,
    batch_id: int,
    partition_cols: Sequence[str] = (),
) -> None:
    """
    Append *df* to a Delta table at most once per (app_id, batch_id).

    Delta records the highest ``txnVersion`` committed for each ``txnAppId``
    in the transaction log and silently skips any write whose version is not
    greater, so a replayed micro-batch becomes a no-op.
    """
    writer = (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .option("txnAppId", app_id)
        .option("txnVersion", batch_id)
    )
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)


def merge_insert_only(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    keys: Sequence[str],
    prune_col: Optional[str] = None,
) -> None:
    """
    ``MERGE ... WHEN NOT MATCHED THEN INSERT`` — insert rows whose key is new.

    Replaying the same rows inserts nothing.  Delta executes an insert-only
    merge as a pure append (no file rewrites), so the target remains a valid
    append-only streaming source for the next layer.
    """
    prune = _prune_predicate("t", prune_col, partition_values(df, prune_col)) if prune_col else None
    (
        DeltaTable.forPath(spark, path).alias("t")
        .merge(df.alias("s"), _merge_condition(keys, prune))
        .whenNotMatchedInsertAll()
        .execute()
    )


def merge_upsert(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    keys: Sequence[str],
    prune_col: Optional[str] = None,
    update_condition: Optional[str] = None,
) -> None:
    """
    ``MERGE`` upsert: update matched rows (optionally only when
    *update_condition* holds) and insert new ones.
    """
    prune = _prune_predicate("t", prune_col, partition_values(df, prune_col)) if prune_col else None
    (
        DeltaTable.forPath(spark, path).alias("t")
        .merge(df.alias("s"), _merge_condition(keys, prune))
        .whenMatchedUpdateAll(condition=update_condition)
        .whenNotMatchedInsertAll()
        .execute()
    )


def replace_scope(
    spark: SparkSession,
    recomputed: DataFrame,
    path: str,
    scope_keys: DataFrame,
    scope_cols: Sequence[str],
    merge_keys: Sequence[str],
    partition_cols: Sequence[str] = (),
    prune_col: Optional[str] = None,
) -> None:
    """
    Make the target's rows for *scope_keys* exactly equal to *recomputed*.

    Used by Gold: for every key touched by a micro-batch (e.g. a
    ``(date, repo_name)`` pair) the aggregate is recomputed from Silver, then
    merged in a single atomic commit that

    * updates rows that still exist,
    * inserts rows that are new,
    * deletes target rows inside the scope that no longer have a recomputed
      row (e.g. a repo whose only event in a week was removed).

    Because the result is a pure function of Silver, replaying the batch
    yields the same table — the write is idempotent.
    """
    recomputed = recomputed.cache()
    try:
        if not DeltaTable.isDeltaTable(spark, path):
            writer = recomputed.write.format("delta").mode("overwrite")
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            writer.save(path)
            return

        target = spark.read.format("delta").load(path)
        missing = [c for c in recomputed.columns if c not in target.columns]
        if missing:
            raise ValueError(
                f"Delta table {path} is missing columns {missing}; rebuild it once "
                "with the batch Gold job (run_pipeline.py --gold) before streaming."
            )

        # Target rows inside the scope that the recompute no longer produces.
        # They keep their real values so the partition-pruning predicate
        # below still covers them.
        stale = (
            target.join(scope_keys.select(*scope_cols).distinct(), list(scope_cols), "left_semi")
            .join(recomputed.select(*merge_keys), list(merge_keys), "left_anti")
            .select(*recomputed.columns)
        )

        source = recomputed.withColumn("_delete", F.lit(False)).unionByName(
            stale.withColumn("_delete", F.lit(True))
        )

        prune = None
        if prune_col:
            prune = _prune_predicate("t", prune_col, partition_values(source, prune_col))

        assignments = {c: F.col(f"s.{c}") for c in recomputed.columns}
        (
            DeltaTable.forPath(spark, path).alias("t")
            .merge(source.alias("s"), _merge_condition(merge_keys, prune))
            .whenMatchedDelete(condition="s._delete")
            .whenMatchedUpdate(set=assignments)
            .whenNotMatchedInsert(condition="NOT s._delete", values=assignments)
            .execute()
        )
    finally:
        recomputed.unpersist()
