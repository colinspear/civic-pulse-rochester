"""
Pull building-permit records from Buffalo’s Socrata feed **9p2d-f3yt** and
write Parquet to
    s3://$BUCKET/raw/buf_permits/year=YYYY/month=MM/day=DD/part-0.parquet

Environment
-----------
AWS_REGION, BUCKET               (required)
SOCRATA_APP_TOKEN                (optional)
TARGET_DATE  = YYYY-MM-DD        (optional – exact calendar day)
LOOKBACK_DAYS = <int>            (default 1 – yesterday style)
"""
from __future__ import annotations

import datetime as _dt
import os, sys, requests, pandas as pd, pyarrow as pa, pyarrow.parquet as pq, boto3
from utils.geocode import census_batch_geocode

BASE   = "https://data.buffalony.gov/resource/9p2d-f3yt.json"
TOKEN  = os.getenv("SOCRATA_APP_TOKEN", "")
FIELDS = [
    "apno",
    "aptype",
    "issued",
    "stname",
    "city",
    "state",
    "zip",
    "latitude",
    "longitude",
    "value",
]
LIMIT = 50_000

# ---------------------------------------------------------------------------
# Date window
# ---------------------------------------------------------------------------
_target = os.getenv("TARGET_DATE")
_default_lookback = 1

if _target:
    day      = pd.to_datetime(_target).date()
    start_dt = day
    end_dt   = day + _dt.timedelta(days=1)
    y, m, d  = day.year, f"{day.month:02}", f"{day.day:02}"
    key = f"raw/buf_permits/year={y}/month={m}/day={d}/part-0.parquet"
else:
    lookback = int(os.getenv("LOOKBACK_DAYS", str(_default_lookback)))
    start_dt = (_dt.datetime.utcnow() - _dt.timedelta(days=lookback)).date()
    end_dt   = _dt.datetime.utcnow().date()
    ymd      = _dt.datetime.utcnow().strftime("year=%Y/month=%m/day=%d")
    key = f"raw/buf_permits/{ymd}/part-0.parquet"

where = f"issued >= '{start_dt.isoformat()}'"
if _target:
    where += f" AND issued < '{end_dt.isoformat()}'"

params_base = {
    "$select": ", ".join(FIELDS),
    "$where":  where,
    "$limit":  LIMIT,
}
headers = {"X-App-Token": TOKEN} if TOKEN else {}

# ---------------------------------------------------------------------------
# Pagination loop
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
        break
    offset += LIMIT

if not rows:
    print("No Permit rows.")
    sys.exit(0)

# ---------------------------------------------------------------------------
# DataFrame & geocoding
# ---------------------------------------------------------------------------
df = pd.DataFrame(rows)
df["pulled_utc"] = pd.Timestamp.utcnow()

for col in FIELDS:
    if col not in df:
        if col == "issued":
            df[col] = pd.NaT
        elif col in {"latitude", "longitude", "value"}:
            df[col] = pd.NA
        else:
            df[col] = pd.NA

# type cast
df["issued"] = pd.to_datetime(df["issued"], errors="coerce").dt.date
for ncol in ("latitude", "longitude", "value"):
    df[ncol] = pd.to_numeric(df[ncol], errors="coerce")

df["pulled_utc"] = pd.to_datetime(df["pulled_utc"], utc=True)

df.rename(columns={"latitude": "latitude_orig", "longitude": "longitude_orig"}, inplace=True)
df.reset_index(inplace=True, names="id")

geo_df = census_batch_geocode(
    df[["id", "stname", "city", "state", "zip"]],
    id_col="id",
    addr_col=["stname", "city", "state", "zip"],
)

df = df.merge(geo_df, on="id", how="left", suffixes=("_orig", ""))

for col in ("latitude_orig", "longitude_orig", "latitude", "longitude"):
    df[col] = pd.to_numeric(df[col], errors="coerce")

# ---------------------------------------------------------------------------
# Local debug option
# ---------------------------------------------------------------------------
if os.getenv("BUCKET") == "LOCAL":
    out = f"permits_test_{_dt.datetime.utcnow().strftime('%Y-%m-%d')}.csv"
    df.to_csv(out, index=False)
    print(f"Wrote {len(df):,} rows → {out}")
    sys.exit(0)

# ---------------------------------------------------------------------------
# Parquet → S3
# ---------------------------------------------------------------------------
_schema = pa.schema([
    ("apno",           pa.string()),
    ("aptype",         pa.string()),
    ("issued",         pa.date32()),
    ("stname",         pa.string()),
    ("city",           pa.string()),
    ("state",          pa.string()),
    ("zip",            pa.string()),
    ("value",          pa.float64()),
    ("latitude_orig",  pa.float64()),
    ("longitude_orig", pa.float64()),
    ("latitude",       pa.float64()),
    ("longitude",      pa.float64()),
    ("match_ok",       pa.bool_()),
    ("pulled_utc",     pa.timestamp("us")),
])

table = pa.Table.from_pandas(df, schema=_schema)
buf   = pa.BufferOutputStream()
pq.write_table(table, buf, compression="zstd")

boto3.client("s3", region_name=os.getenv("AWS_REGION")).put_object(
    Bucket=os.getenv("BUCKET"), Key=key, Body=buf.getvalue().to_pybytes())
print(f"Wrote {len(df):,} Permit rows → s3://{os.getenv('BUCKET')}/{key}")
