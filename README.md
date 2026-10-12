# GitHub Activity Lakehouse

A production-style PySpark + Delta Lake pipeline that ingests **GH Archive** data
through the Medallion architecture: **Bronze → Silver → Gold**.

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
                               │  ingest_to_bronze()
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  BRONZE  data/bronze/   (Delta Lake, partitioned by date)            │
│  • Raw events, schema-on-read                                        │
│  • Adds _ingested_at audit column                                    │
│  • Deduplicates within each batch on event id                        │
└─────────────────────────────┬────────────────────────────────────────┘
                               │  bronze_to_silver()
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  SILVER  data/silver/   (Delta Lake, partitioned by date)            │
│  • Explicit PySpark schema applied (GH_EVENT_SCHEMA)                 │
│  • Global dedup via MERGE INTO on event id (handles late data)       │
│  • Computed columns: date, repo_name, actor_login, event_type        │
│  • Adds _processed_at audit column                                   │
│  • Data-quality checks + quarantine for bad records                  │
└────────────────────┬──────────────────────────────────────┬──────────┘
                     │  daily_active_repos()                 │
                     │  pr_merge_time()                      │
                     │  top_growing_repos()                  │
                     │  contributor_retention()              │
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
│   ├── bronze/ingest.py         # Bronze ingestion (batch + streaming)
│   ├── silver/schema.py         # Explicit GH event StructType
│   ├── silver/transform.py      # Bronze → Silver transform + MERGE INTO
│   ├── silver/dq.py             # Data quality checks + quarantine
│   ├── gold/aggregations.py     # Four Gold aggregation tables
│   └── utils/spark_session.py   # SparkSession factory with Delta config
├── tests/
│   ├── conftest.py              # Shared pytest fixtures (spark, sample_df)
│   ├── fixtures/sample_events.json  # 10 realistic fake GH events
│   ├── test_bronze.py
│   ├── test_silver.py
│   └── test_gold.py
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

# 3. Download 3 days of data (~36 × 50 MB files)
make download

# 4. Run the full pipeline
make pipeline
```

### Manual pipeline run

```bash
# Download a specific date range
PYTHONPATH=. python scripts/download_data.py --start 2024-01-01 --end 2024-01-07

# Run individual stages
PYTHONPATH=. python scripts/run_pipeline.py --bronze
PYTHONPATH=. python scripts/run_pipeline.py --silver --dq
PYTHONPATH=. python scripts/run_pipeline.py --gold

# Or all at once
PYTHONPATH=. python scripts/run_pipeline.py --bronze --silver --dq --gold
```

---

## Layer Details

### Bronze — Raw Ingestion
- **Source**: GH Archive NDJSON files downloaded to `data/raw/`
- **Writer**: Spark batch read (`readStream` with `availableNow=True` for streaming mode)
- **Output**: Delta table at `data/bronze/`, partitioned by `date`
- **Transforms**: adds `_ingested_at`, derives `date` from `created_at`, deduplicates within each batch
- **Schema**: schema-on-read (Spark infers types)

### Silver — Clean & Typed
- **Source**: Bronze Delta table
- **Writer**: `MERGE INTO` on `id` — handles late-arriving duplicates, reruns are idempotent
- **Output**: Delta table at `data/silver/`, partitioned by `date`
- **Transforms**: explicit `GH_EVENT_SCHEMA` applied, computes `repo_name`, `actor_login`, `event_type`, adds `_processed_at`
- **DQ checks**: null_rate, schema drift, row count, duplicate rate, quarantine of bad records

### Gold — Aggregations
| Table | Description |
|-------|-------------|
| `daily_active_repos` | Events per repo per day + distinct actor count |
| `pr_merge_time` | Hours from PR creation to merge for merged PRs |
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
- **Bronze**: batch read, audit columns, date partitioning, dedup within batch, null-id filtering
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
   from src.bronze.ingest import ingest_to_bronze
   from src.silver.transform import bronze_to_silver
   from src.gold.aggregations import daily_active_repos, pr_merge_time

   spark = get_spark(local=False)  # use existing Databricks cluster session
   ingest_to_bronze(spark, use_streaming=True)   # use Auto Loader for streaming
   bronze_to_silver(spark)
   daily_active_repos(spark)
   ```

6. **Optimise with Delta features**:
   ```python
   spark.sql("OPTIMIZE delta.`dbfs:/mnt/github-lakehouse/silver/` ZORDER BY (repo_name, date)")
   spark.sql("VACUUM delta.`dbfs:/mnt/github-lakehouse/silver/` RETAIN 168 HOURS")
   ```
