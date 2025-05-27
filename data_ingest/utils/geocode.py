from __future__ import annotations

import csv
import io
import random
import time
from typing import List

import pandas as pd
import requests
from requests.exceptions import RequestException

# ---------------------------------------------------------------------------
# Census Batch‑Geocoding API constants
# ---------------------------------------------------------------------------
CENSUS_BATCH_URL = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"
BENCHMARK = "Public_AR_Current"           # same benchmark we have been using
MAX_BATCH = 10_000                         # hard API limit documented by Census
DEFAULT_CHUNK = 2_000                      # practical chunk to avoid connection drops
MAX_RETRIES = 5                            # retry attempts for transient failures

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _post_with_retry(url: str, *, files: dict, data: dict, timeout: int = 60):
    """POST with exponential back‑off retries for flaky Census servers."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return requests.post(url, files=files, data=data, timeout=timeout)
        except RequestException as exc:
            if attempt == MAX_RETRIES:
                raise  # bubble up final failure
            wait = 2 ** attempt + random.random()
            print(f"⚠️  {exc} – retrying in {wait:.1f}s (attempt {attempt}/{MAX_RETRIES})")
            time.sleep(wait)


def _build_batch_csv(df: pd.DataFrame, id_col: str, addr_cols: List[str]) -> str:
    """Return an in‑memory CSV string matching the Census batch spec."""
    buff = io.StringIO()
    writer = csv.writer(buff, lineterminator="\n")
    for _, row in df[[id_col] + addr_cols].iterrows():
        record: list[str] = [row[id_col]]
        record += [row[col] if pd.notnull(row[col]) else "" for col in addr_cols]
        # Pad to exactly 5 columns (ID + 4 address components) as the API expects
        record += [""] * (5 - len(record))
        writer.writerow(record)
    buff.seek(0)
    return buff.getvalue()


def _geocode_chunk(chunk: pd.DataFrame, *, id_col: str, addr_cols: List[str]) -> pd.DataFrame:
    """Geocode a dataframe chunk (≤10k rows) and return parsed results."""
    payload_csv = _build_batch_csv(chunk, id_col, addr_cols)

    files = {"addressFile": ("addrs.csv", payload_csv)}
    data = {"benchmark": BENCHMARK}

    resp = _post_with_retry(CENSUS_BATCH_URL, files=files, data=data)

    # Raise for non‑2xx after retries, show first 500 chars for context
    try:
        resp.raise_for_status()
    except requests.HTTPError as exc:
        print("STATUS:", resp.status_code)
        print("RESPONSE TEXT:", resp.text[:500])
        raise exc

    # Parse CSV response
    out = pd.read_csv(
        io.StringIO(resp.text),
        header=None,
        names=[
            "id",
            "input_addr",
            "match",
            "match_type",
            "matched_addr",
            "coordinates",
            "tiger_id",
            "side",
        ],
    )

    out["latitude"] = out["coordinates"].str.split(",").str[1].astype(float)
    out["longitude"] = out["coordinates"].str.split(",").str[0].astype(float)
    out["match_ok"] = out["match"].eq("Match")

    return out[["id", "latitude", "longitude", "match_ok"]]


# ---------------------------------------------------------------------------
# Public function – supports automatic chunking
# ---------------------------------------------------------------------------

def census_batch_geocode(
    df: pd.DataFrame,
    *,
    id_col: str,
    addr_col: str | List[str],
    chunk_size: int = DEFAULT_CHUNK,
) -> pd.DataFrame:
    """Batch‑geocode **any length** DataFrame via the Census batch API.

    Parameters
    ----------
    df : DataFrame with one row per address
    id_col : Column containing unique IDs (string or numeric)
    addr_col : Either a single column (full address) or a list of columns
               corresponding to street, city, state, zip
    chunk_size : Rows per API call (must be ≤ ``MAX_BATCH``)

    Returns
    -------
    DataFrame with columns ``[id_col, latitude, longitude, match_ok]``.
    """
    if isinstance(addr_col, str):
        addr_cols = [addr_col]
    else:
        addr_cols = list(addr_col)

    missing = [c for c in [id_col] + addr_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    if chunk_size > MAX_BATCH:
        raise ValueError(f"chunk_size must be ≤{MAX_BATCH}; got {chunk_size}")

    results: list[pd.DataFrame] = []

    for start in range(0, len(df), chunk_size):
        chunk = df.iloc[start : start + chunk_size]
        # Retry chunk‑level failures so a single bad group doesn't halt backfill
        try:
            res = _geocode_chunk(chunk, id_col=id_col, addr_cols=addr_cols)
            results.append(res)
        except Exception as exc:
            print(f"❌  Failed chunk rows {start}:{start + len(chunk)} – {exc}")
            raise  # re‑raise after logging; adjust if you prefer to skip
        time.sleep(1)  # polite pause between calls

    return pd.concat(results, ignore_index=True)
