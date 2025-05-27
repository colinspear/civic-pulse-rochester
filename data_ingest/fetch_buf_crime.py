"""
Pull crime incidents from Buffalo’s Socrata feed **d6g9-xbgu** and save to

    s3://$BUCKET/raw/buf_crime/year=YYYY/month=MM/day=DD/part-0.parquet

Environment
-----------
AWS_REGION, BUCKET               (required)
SOCRATA_APP_TOKEN                (optional – increases quota)
TARGET_DATE  = YYYY-MM-DD        (optional – exact calendar day)
LOOKBACK_DAYS = <int>            (default 1 – yesterday style)
"""
from __future__ import annotations

import datetime as _dt
import os, sys, requests, pandas as pd, pyarrow as pa, pyarrow.parquet as pq, boto3

BASE  = "https://data.buffalony.gov/resource/d6g9-xbgu.json"
TOKEN = os.getenv("SOCRATA_APP_TOKEN", "")
FIELDS = [
    "case_number",
    "incident_datetime",
    "incident_type_primary",
    "parent_incident_type",
    "latitude",
    "longitude",
]
LIMIT = 50_000  # Socrata page size

# ---------------------------------------------------------------------------
# Date window logic
# ---------------------------------------------------------------------------
_target = os.getenv("TARGET_DATE")
_default_lookback = 1

if _target:  # single calendar day
    day = pd.to_datetime(_target).date()
    start_dt = day
    end_dt   = day + _dt.timedelta(days=1)
    y, m, d  = day.year, f"{day.month:02}", f"{day.day:02}"
    key = f"raw/buf_crime/year={y}/month={m}/day={d}/part-0.parquet"
else:
    lookback = int(os.getenv("LOOKBACK_DAYS", str(_default_lookback)))
    start_dt = (_dt.datetime.utcnow() - _dt.timedelta(days=lookback)).date()
    end_dt   = _dt.datetime.utcnow().date()
    ymd      = _dt.datetime.utcnow().strftime("year=%Y/month=%m/day=%d")
    key = f"raw/buf_crime/{ymd}/part-0.parquet"

start_iso = start_dt.isoformat()
where = f"incident_datetime >= '{start_iso}'"
if _target:
    where += f" AND incident_datetime < '{end_dt.isoformat()}'"  # upper bound single day

params_base = {
    "$select": ", ".join(FIELDS),
    "$where":  where,
    "$limit":  LIMIT,
}
headers = {"X-App-Token": TOKEN} if TOKEN else {}

# ---------------------------------------------------------------------------
# Paginated fetch loop
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
    print("No crime rows.")
    sys.exit(0)

# ---------------------------------------------------------------------------
# DataFrame & typing
# ---------------------------------------------------------------------------
df = pd.DataFrame(rows)

df["pulled_utc"] = pd.Timestamp.utcnow()

for col in FIELDS:
    if col not in df:
        if col == "incident_datetime":
            df[col] = pd.NaT
        elif col in {"latitude", "longitude"}:
            df[col] = pd.NA
        else:
            df[col] = pd.NA

# cast
for tcol in ("incident_datetime", "pulled_utc"):
    df[tcol] = pd.to_datetime(df[tcol], utc=True, errors="coerce")
for ncol in ("latitude", "longitude"):
    df[ncol] = pd.to_numeric(df[ncol], errors="coerce")

# ---------------------------------------------------------------------------
# Local CSV debug option
# ---------------------------------------------------------------------------
if os.getenv("BUCKET") == "LOCAL":
    out = f"crime_test_{_dt.datetime.utcnow().strftime('%Y-%m-%d')}.csv"
    df.to_csv(out, index=False)
    print(f"Wrote {len(df):,} rows → {out}")
    sys.exit(0)

# ---------------------------------------------------------------------------
# Parquet serialization
# ---------------------------------------------------------------------------
_schema = pa.schema([
    ("case_number",           pa.string()),
    ("incident_datetime",     pa.timestamp("us")),
    ("incident_type_primary", pa.string()),
    ("parent_incident_type",  pa.string()),
    ("latitude",              pa.float64()),
    ("longitude",             pa.float64()),
    ("pulled_utc",            pa.timestamp("us")),
])

table = pa.Table.from_pandas(df, schema=_schema)
buf   = pa.BufferOutputStream()
pq.write_table(table, buf, compression="zstd")

boto3.client("s3", region_name=os.getenv("AWS_REGION")).put_object(
    Bucket=os.getenv("BUCKET"), Key=key, Body=buf.getvalue().to_pybytes())
print(f"Wrote {len(df):,} crime rows → s3://{os.getenv('BUCKET')}/{key}")
