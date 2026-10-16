"""
GH Archive downloader.

Kept free of Spark imports so ``scripts/download_data.py`` runs in any Python
environment that has ``requests``.

Public API
----------
    download_gharchive(date_str, hour, raw_path, overwrite) -> local file path
"""

from __future__ import annotations

import logging
import os
import shutil
from typing import Optional

import requests

logger = logging.getLogger(__name__)

_GHARCHIVE_URL_TEMPLATE = "https://data.gharchive.org/{date}-{hour}.json.gz"


def download_gharchive(
    date_str: str,
    hour: int,
    raw_path: Optional[str] = None,
    overwrite: bool = False,
) -> str:
    """
    Download one hourly GH Archive file.

    Parameters
    ----------
    date_str:
        ISO date string, e.g. ``"2024-01-15"``.
    hour:
        Hour 0–23.
    raw_path:
        Local directory to save the file.  Defaults to ``data/raw``.
    overwrite:
        When False (default) skips download if the file already exists.

    Returns
    -------
    str
        Absolute local path of the downloaded (or pre-existing) file.
    """
    from conf.config import RAW_PATH

    raw_path = raw_path or RAW_PATH
    os.makedirs(raw_path, exist_ok=True)

    filename = f"{date_str}-{hour}.json.gz"
    dest = os.path.join(raw_path, filename)

    if os.path.exists(dest) and not overwrite:
        logger.info("Already exists, skipping: %s", dest)
        return dest

    url = _GHARCHIVE_URL_TEMPLATE.format(date=date_str, hour=hour)
    logger.info("Downloading %s → %s", url, dest)

    # Download to a hidden temp file and rename atomically.  The Bronze stream
    # watches raw_path, and Spark's file source ignores names starting with
    # "." — so it can never pick up a half-written file.
    tmp = os.path.join(raw_path, f".{filename}.part")
    with requests.get(url, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        with open(tmp, "wb") as fh:
            shutil.copyfileobj(resp.raw, fh)
    os.replace(tmp, dest)

    logger.info("Saved %s (%.1f MB)", dest, os.path.getsize(dest) / 1_048_576)
    return dest
