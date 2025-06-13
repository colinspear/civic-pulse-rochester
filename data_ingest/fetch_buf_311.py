"""
Pull one day (or rolling window) of 311 service‑request records from
Buffalo’s Socrata feed **3tj7‑3tdz** and write Parquet to

    s3://$BUCKET/raw/buf_311/year=YYYY/month=MM/day=DD/part-0.parquet

Environment
-----------
AWS_REGION, BUCKET  (required)
SOCRATA_APP_TOKEN   (optional – but recommended to avoid public‑quota throttling)
TARGET_DATE         (YYYY‑MM‑DD → pull exactly that calendar day)
LOOKBACK_DAYS       (integer, default 1 → "yesterday" style backfill)
"""
from __future__ import annotations

import datetime as _dt
import os, sys, requests, pandas as pd, pyarrow as pa, pyarrow.parquet as pq, boto3

# ---------------------------------------------------------------------------
# Constants & configuration
# ---------------------------------------------------------------------------
BASE   = "https://data.buffalony.gov/resource/3tj7-3tdz.json"
TOKEN  = os.getenv("SOCRATA_APP_TOKEN", "")
FIELDS = [
    "createddate",
    "casenumber",
    "department",
    "division",
    "type",
    "status",
    "statusdescription",
    "closeddate",
    "latitude",
    "longitude",
]
LIMIT  = 50_000   # Socrata hard limit per request

# ---------------------------------------------------------------------------
# Date window logic (single‑day or rolling look‑back)
# ---------------------------------------------------------------------------
_target = os.getenv("TARGET_DATE")  # "YYYY-MM-DD" or None
_lookback_default = 1

if _target:  # ----- explicit single‑day backfill --------------------------------
    target_dt = pd.to_datetime(_target).date()
    start_dt  = target_dt
    end_dt    = target_dt + _dt.timedelta(days=1)  # exclusive upper bound
    y, m, d   = target_dt.year, f"{target_dt.month:02}", f"{target_dt.day:02}"
    key = f"raw/buf_311/year={y}/month={m}/day={d}/part-0.parquet"
else:        # ----- rolling look‑back (default yesterday) -----------------------
    lookback_days = int(os.getenv("LOOKBACK_DAYS", str(_lookback_default)))
    start_dt      = (_dt.datetime.utcnow() - _dt.timedelta(days=lookback_days)).date()
    end_dt        = _dt.datetime.utcnow().date()  # now (exclusive) – gives open interval
    ymd           = _dt.datetime.utcnow().strftime("year=%Y/month=%m/day=%d")
    key = f"raw/buf_311/{ymd}/part-0.parquet"

# Build Socrata $where clause
start_iso = start_dt.isoformat()
where = f"createddate >= '{start_iso}'"
if _target:
    end_iso = end_dt.isoformat()
    where += f" AND createddate < '{end_iso}'"  # single‑day upper bound

params_base = {
    "$select": ", ".join(FIELDS),
    "$where":  where,
    "$limit":  LIMIT,
}

headers = {"X-App-Token": TOKEN} if TOKEN else {}

# ---------------------------------------------------------------------------
# Paginated fetch (handles >50k rows safely)
# ---------------------------------------------------------------------------
rows, offset = [], 0
while True:
    resp = requests.get(BASE,
                        params={**params_base, "$offset": offset},
                        headers=headers,
                        timeout=60)
    resp.raise_for_status()
    batch = resp.json()
    if not batch:
        break
    rows.extend(batch)
    if len(batch) < LIMIT:
        break  # last page fetched
    offset += LIMIT

if not rows:
    print("No 311 rows.")
    sys.exit(0)

# ---------------------------------------------------------------------------
# DataFrame construction & type coercion
# ---------------------------------------------------------------------------
df = pd.DataFrame(rows)

df["pulled_utc"] = pd.Timestamp.utcnow()
df["city"] = df["city"].fillna("Buffalo")
df["state"] = df["state"].fillna("New York")

# ensure all expected cols present
for col in FIELDS:
    if col not in df:
        if col in {"createddate", "closeddate"}:
            df[col] = pd.NaT
        elif col in {"latitude", "longitude"}:
            df[col] = pd.NA
        else:
            df[col] = pd.NA

# timestamp cast
for tcol in ("createddate", "closeddate", "pulled_utc"):
    df[tcol] = pd.to_datetime(df[tcol], utc=True, errors="coerce")

# numeric cast
for ncol in ("latitude", "longitude"):
    df[ncol] = pd.to_numeric(df[ncol], errors="coerce")

# ---------------------------------------------------------------------------
# Local debug option ---------------------------------------------------------
# ---------------------------------------------------------------------------
if os.getenv("BUCKET") == "LOCAL":
    out = f"311_test_{_dt.datetime.utcnow().strftime('%Y-%m-%d')}.csv"
    df.to_csv(out, index=False)
    print(f"Wrote {len(df):,} rows → {out}")
    sys.exit(0)

# ---------------------------------------------------------------------------
# Parquet → S3
# ---------------------------------------------------------------------------
_schema = pa.schema([
    ("createddate",       pa.timestamp("us")),
    ("casenumber",        pa.string()),
    ("department",        pa.string()),
    ("division",          pa.string()),
    ("type",              pa.string()),
    ("status",            pa.string()),
    ("statusdescription", pa.string()),
    ("closeddate",        pa.timestamp("us")),
    ("latitude",          pa.float64()),
    ("longitude",         pa.float64()),
    ("pulled_utc",        pa.timestamp("us")),
])

table = pa.Table.from_pandas(df, schema=_schema)
buf   = pa.BufferOutputStream()
pq.write_table(table, buf, compression="zstd")

boto3.client("s3", region_name=os.getenv("AWS_REGION")).put_object(
    Bucket=os.getenv("BUCKET"), Key=key, Body=buf.getvalue().to_pybytes())
print(f"Wrote {len(df):,} 311 rows → s3://{os.getenv('BUCKET')}/{key}")
