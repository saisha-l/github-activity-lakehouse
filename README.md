# GitHub Activity Lakehouse

An incremental **Spark Structured Streaming** pipeline that ingests **GH Archive**
events into **Delta Lake** bronze / silver / gold tables, with per-stream
checkpointing and idempotent `MERGE` upserts for **exactly-once recovery**.
Built to backfill 100M+ events (a month of GH Archive) and then keep up hour
by hour, processing only what is new.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                     GH Archive (data.gharchive.org)                  │
│              Hourly .json.gz  ~50 MB/file  (NDJSON format)           │
└─────────────────────────────┬────────────────────────────────────────┘
                               │  download_gharchive()
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  data/raw/   2024-01-15-0.json.gz, 2024-01-15-1.json.gz, ...        │
└─────────────────────────────┬────────────────────────────────────────┘
                               │  stream 1: bronze_ingest
                               │  file source · maxFilesPerTrigger · ckpt/bronze
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  BRONZE  data/bronze/   (Delta Lake, partitioned by date)            │
│  • Verbatim event in _raw_json + id, type, created_at, _source_file  │
│  • Insert-only MERGE on id → append-only, never duplicated           │
│  • Malformed lines → quarantine/bronze (txnAppId/txnVersion append)  │
└─────────────────────────────┬────────────────────────────────────────┘
                               │  stream 2: silver_transform
                               │  Delta source · ckpt/silver
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  SILVER  data/silver/   (Delta Lake, partitioned by date, CDF on)    │
│  • Explicit PySpark schema applied (GH_EVENT_SCHEMA)                 │
│  • Upsert MERGE on id, pruned to the batch's date partitions         │
│  • Computed columns: date, repo_name, actor_login, event_type        │
│  • Data-quality checks + quarantine for bad records                  │
└────────────────────┬──────────────────────────────────────┬──────────┘
                     │  stream 3: gold_incremental           │
                     │  Change Data Feed · ckpt/gold         │
                     │  recompute touched keys → MERGE       │
                     ▼                                       │
┌────────────────────────────────────────────┐               │
│  GOLD  data/gold/   (Delta Lake)           │               │
│  • daily_active_repos/                     │   DQ checks   │
│  • pr_merge_time/                          │  data/quarantine/
│  • top_growing_repos/                      │               │
│  • contributor_retention/                  │               │
└────────────────────────────────────────────┘               │
```

---

## Project Structure

```
github-activity-lakehouse/
├── .github/workflows/ci.yml     # GitHub Actions CI (Python 3.11, Java 11)
├── conf/config.py               # Central configuration (paths, settings)
├── src/
│   ├── pipeline.py              # Orchestrates the 3 streams (availableNow / continuous)
│   ├── bronze/ingest.py         # Raw files → Bronze stream (insert-only MERGE)
│   ├── silver/schema.py         # Explicit GH event StructType
│   ├── silver/transform.py      # Bronze → Silver stream (upsert MERGE, CDF)
│   ├── silver/dq.py             # Data quality checks + quarantine
│   ├── gold/aggregations.py     # Gold table definitions + batch rebuild
│   ├── gold/incremental.py      # Silver CDF → Gold stream (scoped recompute MERGE)
│   ├── utils/streaming.py       # Triggers, idempotent MERGE / append helpers
│   └── utils/spark_session.py   # SparkSession factory with Delta config
├── tests/
│   ├── conftest.py              # Shared pytest fixtures (spark, sample_df)
│   ├── fixtures/sample_events.json  # 10 realistic fake GH events
│   ├── test_bronze.py
│   ├── test_silver.py
│   ├── test_gold.py
│   └── test_streaming.py        # Incremental runs + crash/replay recovery
├── scripts/
│   ├── download_data.py         # CLI to download GH Archive files
│   └── run_pipeline.py          # CLI to run pipeline stages
├── requirements.txt
├── Makefile
└── README.md
```

---

## Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/your-org/github-activity-lakehouse.git
cd github-activity-lakehouse

# 2. Install dependencies (requires Python 3.11, Java 11)
make install

# 3. Download 3 days of data (72 hourly files)
make download

# 4. Run the incremental pipeline (processes only new data, then exits)
make pipeline

# Full-month backfill (~744 hourly files, 100M+ events), streamed in
# bounded micro-batches; override the window with BACKFILL_START / BACKFILL_END
make backfill
```

### Manual pipeline run

```bash
# Download a specific date range
PYTHONPATH=. python scripts/download_data.py --start 2024-01-01 --end 2024-01-07

# Incremental: drain whatever is new through all three streams, then exit.
# Re-run after every download (cron / Airflow / Databricks Job).
PYTHONPATH=. python scripts/run_pipeline.py --streaming --bronze --silver --gold --stats

# Long-running: all three streams concurrently, 1-minute micro-batches
PYTHONPATH=. python scripts/run_pipeline.py --streaming --bronze --silver --gold \
    --trigger-interval "1 minute"

# Batch full rebuild / repair (idempotent; does not touch checkpoints)
PYTHONPATH=. python scripts/run_pipeline.py --bronze --silver --dq --gold
```

---

## Incremental Processing & Exactly-Once Recovery

| Stream | Source | Sink write | Checkpoint |
|--------|--------|-----------|------------|
| `bronze_ingest` | `data/raw/` file source (`maxFilesPerTrigger`, default 24 = 1 day) | `MERGE … WHEN NOT MATCHED INSERT` on `id` | `checkpoints/bronze` |
| `silver_transform` | Bronze Delta table | `MERGE` upsert on `id`, update only if `_ingested_at` is newer | `checkpoints/silver` |
| `gold_incremental` | Silver **Change Data Feed** | recompute touched keys → `MERGE` (update / insert / delete-stale) | `checkpoints/gold` |

**Why it's exactly-once.** Structured Streaming checkpoints record each
micro-batch's source offsets before it runs and a commit marker after it
finishes. If the job dies between the Delta commit and the checkpoint commit,
the restart replays *the same batch with the same input*. Every sink write is
idempotent, so the replay changes nothing:

* **Bronze**: insert-only MERGE, so already-present ids are skipped. Delta runs an
  insert-only merge as a pure append, so Bronze stays a valid append-only
  streaming source. Quarantine appends carry `txnAppId`/`txnVersion=batchId`,
  and Delta drops a write whose version it has already committed.
* **Silver**: matched rows update only when `source._ingested_at > target._ingested_at`.
  A replay matches every row and updates none.
* **Gold**: each batch recomputes the affected `(date, repo)`, `event_id`,
  `repo` and `actor` slices from Silver with the same functions as the batch
  rebuild, then MERGEs them in. The output is a pure function of Silver, so a
  replay rewrites identical values.

`tests/test_streaming.py` checks this directly. It deletes the newest
`commits/<id>` file from each checkpoint (a crash after the sink commit), restarts,
and asserts the tables are unchanged. It also checks that incrementally
maintained Gold equals a full batch rebuild.

**Scaling notes**

* GH Archive files are gzip and can't be split (one task per file).
  `maxFilesPerTrigger` bounds each micro-batch, so memory stays flat however
  long the backlog is.
* MERGE conditions include a literal `date IN (...)` predicate built from the
  batch, so Delta prunes the target down to the 1–2 partitions a batch
  touches instead of scanning the whole 100M+ row table.
* Downloads land as hidden `.part` files and are renamed atomically, so the
  file stream never reads a half-written file.
* On first start the Gold CDF stream reads the full Silver snapshot, so no
  separate Gold backfill is needed.

---

## Layer Details

### Bronze — Raw Ingestion
- **Source**: GH Archive NDJSON files downloaded to `data/raw/`
- **Reader**: `readStream.format("text")` with a checkpoint, so each file is read once
- **Writer**: `foreachBatch` → insert-only `MERGE` on `id` (pruned by `date`)
- **Output**: Delta table at `data/bronze/`, partitioned by `date`
- **Columns**: `id`, `type`, `created_at`, `date`, `_raw_json` (verbatim event), `_source_file`, `_ingested_at`
- **Bad lines**: no `id` / `created_at` → `data/quarantine/bronze/`

### Silver — Clean & Typed
- **Source**: Bronze Delta table (streaming source)
- **Writer**: `foreachBatch` → `MERGE INTO` on `id`, so late duplicates, reruns and replays are idempotent
- **Change Data Feed**: enabled (`delta.enableChangeDataFeed=true`) to drive incremental Gold
- **Output**: Delta table at `data/silver/`, partitioned by `date`
- **Transforms**: explicit `GH_EVENT_SCHEMA` applied, computes `repo_name`, `actor_login`, `event_type`, adds `_processed_at`
- **DQ checks**: null_rate, schema drift, row count, duplicate rate, quarantine of bad records

### Gold — Aggregations
Maintained incrementally by `gold/incremental.py`. `make rebuild` recomputes everything in batch from the same definitions.

| Table | Description |
|-------|-------------|
| `daily_active_repos` | Events per repo per day + distinct actor count |
| `pr_merge_time` | Hours from PR creation to merge for merged PRs (keyed by `event_id`) |
| `top_growing_repos` | WatchEvent (star) counts per week + week-over-week growth |
| `contributor_retention` | Actors active in 2+ consecutive weeks flagged as retained |

---

## Performance Tuning

### Z-Ordering
Apply Z-ordering on the most common query predicates to co-locate related data:
```sql
OPTIMIZE delta.`data/silver/` ZORDER BY (repo_name, actor_login);
OPTIMIZE delta.`data/gold/daily_active_repos/` ZORDER BY (repo_name);
```

### Adaptive Query Execution (AQE)
AQE is enabled by default in this project (`spark.sql.adaptive.enabled=true`).
It handles:
- **Coalescing shuffle partitions** — avoids thousands of tiny partitions after joins
- **Skew join handling** — automatically splits oversized partitions from repos like `torvalds/linux`

### Skew Handling
GH Archive data is inherently skewed — a handful of popular repos (e.g. `microsoft/vscode`) 
generate orders of magnitude more events than average repos.  Mitigations:
- `spark.sql.adaptive.skewJoin.enabled=true` (default in this project)
- Salt the join key with `F.concat(F.col("repo_name"), (F.rand() * 10).cast("int"))` for
  heavy aggregations on repo_name
- Use broadcast joins for small lookup tables (< 10 MB)

### Partitioning Strategy
- **Bronze / Silver**: partitioned by `date` — typical queries filter by date range
- **Gold**: most Gold tables are small enough to skip partitioning; `daily_active_repos` is
  partitioned by `date` for time-range queries

### Shuffle Partitions
Override via env var: `SPARK_SHUFFLE_PARTITIONS=400 make pipeline`  
Default is 200 for single-node; increase to `8 × num_cores` for cluster mode.

---

## Data Quality

The `run_dq_checks()` function in `src/silver/dq.py` validates:

| Check | Threshold | Action on failure |
|-------|-----------|-------------------|
| `null_rate` | < 5% null ids | Raises `DataQualityError` |
| `schema_drift` | All expected columns present | Warning logged |
| `row_count` | > 0 | Warning logged |
| `duplicate_rate` | Informational | Logged |
| Quarantine | null id or null created_at | Written to `data/quarantine/` |

Adjust the threshold: `conf/config.py → DQ_NULL_RATE_THRESHOLD`.

---

## Testing

```bash
# Full test suite
make test

# Specific test module
PYTHONPATH=. pytest tests/test_silver.py -v

# With detailed output on failure
PYTHONPATH=. pytest tests/ -v --tb=long
```

Tests use `tmp_path` fixtures so they never pollute `data/`.

### Test coverage
- **Bronze**: audit columns, date partitioning, dedup, idempotent rerun, null-id quarantine, raw JSON retained
- **Streaming**: only new files are read, cross-file dedup, in-flight downloads ignored, crash-replay idempotency for Bronze / Silver / Gold, incremental Gold == batch rebuild
- **Silver**: global dedup, MERGE idempotency, derived columns, date extraction, DQ null-rate detection, quarantine
- **Gold**: daily_active_repos row uniqueness, pr_merge_time merged-only filter, top_growing_repos star counts, contributor retention logic

---

## How to Run on Databricks

1. **Upload the project** to your Databricks workspace (or clone via Repos).

2. **Create a cluster** with:
   - Databricks Runtime 14.x LTS (includes Spark 3.5 + Delta 3.x)
   - Python 3.11

3. **Install extra libraries** (only `tqdm` and `requests` are needed on Databricks — PySpark and Delta are pre-installed):
   ```
   tqdm==4.66.4
   requests==2.32.3
   ```

4. **Set environment variables** in the cluster config:
   ```
   LAKEHOUSE_ROOT=dbfs:/mnt/github-lakehouse
   ```

5. **Run the pipeline** via a Databricks Job or notebook:
   ```python
   import sys
   sys.path.insert(0, "/Workspace/Repos/your-repo/github-activity-lakehouse")

   from src.utils.spark_session import get_spark
   from src.pipeline import run_incremental_pipeline

   spark = get_spark(local=False)  # use existing Databricks cluster session
   run_incremental_pipeline(spark)   # bronze → silver → gold, availableNow
   ```

6. **Optimise with Delta features**:
   ```python
   spark.sql("OPTIMIZE delta.`dbfs:/mnt/github-lakehouse/silver/` ZORDER BY (repo_name, date)")
   spark.sql("VACUUM delta.`dbfs:/mnt/github-lakehouse/silver/` RETAIN 168 HOURS")
   ```
