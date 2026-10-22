#!/usr/bin/env python3
"""
Chunked GH Archive backfill: download a few days, stream them through
Bronze → Silver → Gold, repeat.

Downloading a whole month up front needs ~60 GB for the raw files alone.
Instead each chunk is downloaded, ingested incrementally (the same
checkpointed ``availableNow`` run as ``make pipeline``), and its raw files are
deleted by the Bronze stream once their micro-batch is committed
(``RAW_CLEAN_SOURCE=delete``).  Peak raw usage stays at about one chunk.

Every chunk is an ordinary incremental run, so the backfill can be stopped
and restarted at any point: finished files are skipped by the downloader
(or already gone) and the checkpoints pick up where they left off.

Usage
-----
    python scripts/backfill.py --start 2024-01-01 --end 2024-01-28
    python scripts/backfill.py --start 2024-01-01 --end 2024-01-28 --chunk-days 2
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _run(cmd: list, env: dict) -> None:
    print(f"\n$ {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, cwd=_PROJECT_ROOT, env=env, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", required=True, type=date.fromisoformat, metavar="YYYY-MM-DD")
    parser.add_argument("--end", required=True, type=date.fromisoformat, metavar="YYYY-MM-DD")
    parser.add_argument("--chunk-days", type=int, default=3, metavar="N",
                        help="Days downloaded and ingested per chunk (default: 3)")
    parser.add_argument("--workers", type=int, default=8, metavar="N",
                        help="Parallel download workers (default: 8)")
    args = parser.parse_args()

    env = dict(os.environ)
    env.setdefault("RAW_CLEAN_SOURCE", "delete")
    env["PYTHONPATH"] = str(_PROJECT_ROOT)
    py = sys.executable

    began = time.perf_counter()
    chunk_start = args.start
    while chunk_start <= args.end:
        chunk_end = min(chunk_start + timedelta(days=args.chunk_days - 1), args.end)
        print(f"\n=== Chunk {chunk_start} → {chunk_end} "
              f"(elapsed {(time.perf_counter() - began) / 60:.1f} min) ===", flush=True)
        _run([py, "scripts/download_data.py", "--start", chunk_start.isoformat(),
              "--end", chunk_end.isoformat(), "--workers", str(args.workers)], env)
        _run([py, "scripts/run_pipeline.py", "--streaming", "--bronze", "--silver", "--gold"], env)
        chunk_start = chunk_end + timedelta(days=1)

    _run([py, "scripts/run_pipeline.py", "--dq", "--stats"], env)
    print(f"\nBackfill finished in {(time.perf_counter() - began) / 60:.1f} min", flush=True)


if __name__ == "__main__":
    main()
