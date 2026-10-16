#!/usr/bin/env python3
"""
Download GH Archive hourly files for a date range.

Usage
-----
    python scripts/download_data.py --start 2024-01-01 --end 2024-01-07
    python scripts/download_data.py --start 2024-01-01 --end 2024-01-01 --hours 0 1 2
    python scripts/download_data.py --start 2024-01-01 --end 2024-01-01 --overwrite

Options
-------
--start DATE      Start date (inclusive), format YYYY-MM-DD
--end   DATE      End date (inclusive), format YYYY-MM-DD
--hours H [H...]  Specific hours to download (default: 0-23)
--raw-path PATH   Override the raw data directory (default: data/raw)
--overwrite       Re-download files even if they already exist
--workers N       Number of parallel download workers (default: 4)
--force           Skip the free-disk-space check
"""

from __future__ import annotations

import argparse
import concurrent.futures
import logging
import os
import shutil
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import List, Tuple

# Ensure project root is on sys.path when run as a script
_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SCRIPT_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from tqdm import tqdm  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Date utilities
# ---------------------------------------------------------------------------


def _date_range(start: date, end: date) -> List[date]:
    """Return a list of dates from *start* to *end* inclusive."""
    days = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def _parse_date(s: str) -> date:
    try:
        return date.fromisoformat(s)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date '{s}'. Expected YYYY-MM-DD.")


# ---------------------------------------------------------------------------
# Download worker
# ---------------------------------------------------------------------------


def _download_one(
    args: Tuple[str, int, str, bool],
) -> Tuple[str, int, bool, str]:
    """
    Worker function called in a thread pool.

    Returns (date_str, hour, success, message).
    """
    date_str, hour, raw_path, overwrite = args
    try:
        from src.bronze.download import download_gharchive

        path = download_gharchive(date_str, hour, raw_path=raw_path, overwrite=overwrite)
        size_mb = os.path.getsize(path) / 1_048_576
        return date_str, hour, True, f"OK ({size_mb:.1f} MB)"
    except Exception as exc:
        return date_str, hour, False, str(exc)


# ---------------------------------------------------------------------------
# Disk-space preflight
# ---------------------------------------------------------------------------

# 2024 hourly files are ~75–125 MB gzipped.  Bronze keeps the raw JSON and
# Silver a typed copy (both compressed Parquet), so budget ~2× the raw size on
# top of the downloads themselves.
_EST_MB_PER_FILE = 100
_LAKEHOUSE_OVERHEAD = 2.0


def _check_disk_space(raw_path: str, tasks: list, overwrite: bool, force: bool) -> None:
    pending = [
        t for t in tasks
        if overwrite or not os.path.exists(os.path.join(raw_path, f"{t[0]}-{t[1]}.json.gz"))
    ]
    need_gb = len(pending) * _EST_MB_PER_FILE * (1 + _LAKEHOUSE_OVERHEAD) / 1024
    free_gb = shutil.disk_usage(raw_path).free / 1024 ** 3
    logger.info(
        "Disk check: %d new files ≈ %.0f GB raw, ≈ %.0f GB incl. Bronze/Silver; %.1f GB free",
        len(pending), need_gb / (1 + _LAKEHOUSE_OVERHEAD), need_gb, free_gb,
    )
    if need_gb > free_gb and not force:
        max_days = free_gb / (24 * _EST_MB_PER_FILE * (1 + _LAKEHOUSE_OVERHEAD) / 1024)
        sys.exit(
            f"Not enough disk space: need ≈ {need_gb:.0f} GB, have {free_gb:.1f} GB "
            f"(≈ {max_days:.1f} day(s) of data fits).  Shrink --start/--end, point "
            "LAKEHOUSE_ROOT at a bigger disk or cloud storage, or pass --force."
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download GH Archive hourly JSON files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--start",
        required=True,
        type=_parse_date,
        metavar="YYYY-MM-DD",
        help="Start date (inclusive)",
    )
    parser.add_argument(
        "--end",
        required=True,
        type=_parse_date,
        metavar="YYYY-MM-DD",
        help="End date (inclusive)",
    )
    parser.add_argument(
        "--hours",
        nargs="+",
        type=int,
        default=list(range(24)),
        metavar="H",
        help="Hours to download (0-23, default: all 24)",
    )
    parser.add_argument(
        "--raw-path",
        default=None,
        metavar="PATH",
        help="Override raw data directory",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-download already-existing files",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Download even if the free-disk-space check fails",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        metavar="N",
        help="Parallel download workers (default: 4)",
    )

    args = parser.parse_args()

    from conf.config import RAW_PATH

    raw_path = args.raw_path or RAW_PATH
    os.makedirs(raw_path, exist_ok=True)

    days = _date_range(args.start, args.end)
    tasks = [
        (d.isoformat(), h, raw_path, args.overwrite)
        for d in days
        for h in sorted(set(args.hours))
    ]

    total = len(tasks)

    _check_disk_space(raw_path, tasks, args.overwrite, args.force)
    logger.info(
        "Downloading %d files  (dates: %s→%s, %d hour(s) each, workers: %d)",
        total,
        args.start,
        args.end,
        len(set(args.hours)),
        args.workers,
    )

    failed = []
    skipped = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(_download_one, t): t for t in tasks}
        with tqdm(total=total, unit="file", desc="Downloading") as pbar:
            for future in concurrent.futures.as_completed(futures):
                date_str, hour, success, msg = future.result()
                pbar.update(1)
                if not success:
                    failed.append((date_str, hour, msg))
                    pbar.write(f"  FAILED  {date_str}-{hour:02d}: {msg}")
                elif "skipping" in msg.lower() or "already" in msg.lower():
                    skipped += 1

    # Summary
    print("\nDownload summary:")
    print(f"  Total tasks : {total}")
    print(f"  Succeeded   : {total - len(failed)}")
    print(f"  Skipped     : {skipped}")
    print(f"  Failed      : {len(failed)}")

    if failed:
        print("\nFailed downloads:")
        for date_str, hour, msg in failed:
            print(f"  {date_str}-{hour:02d}: {msg}")
        sys.exit(1)


if __name__ == "__main__":
    main()
