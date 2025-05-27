"""
Pull *vacant-building* violation cases from Buffalo’s code‑violation feed
**ivrf‑k9vm** and write Parquet to
    s3://$BUCKET/raw/buf_viol/year=YYYY/month=MM/day=DD/part-0.parquet

Environment
-----------
AWS_REGION, BUCKET               (required)
SOCRATA_APP_TOKEN                (optional – raises quota)
TARGET_DATE  = YYYY-MM-DD        (optional – extract that exact day)
LOOKBACK_DAYS = <int>            (default 1 – yesterday style)
"""
from __future__ import annotations

import datetime as _dt
import os, sys, requests, pandas as pd, pyarrow as pa, pyarrow.parquet as pq, boto3

BASE   = "https://data.buffalony.gov/resource/ivrf-k9vm.json"
TOKEN  = os.getenv("SOCRATA_APP_TOKEN", "")
FIELDS = [
    "case_number",
    "date",
    "status",
    "code",
    "code_section",
    "description",
    "address",
    "latitude",
    "longitude",
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
    key = f"raw/buf_viol/year={y}/month={m}/day={d}/part-0.parquet"
else:
    lookback = int(os.getenv("LOOKBACK_DAYS", str(_default_lookback)))
    start_dt = (_dt.datetime.utcnow() - _dt.timedelta(days=lookback)).date()
    end_dt   = _dt.datetime.utcnow().date()
    ymd      = _dt.datetime.utcnow().strftime("year=%Y/month=%m/day=%d")
    key = f"raw/buf_viol/{ymd}/part-0.parquet"

start_iso = start_dt.isoformat()
where = (
    f"date >= '{start_iso}' "
    "AND upper(description) like 'VACANT%'"
)
if _target:
    where += f" AND date < '{end_dt.isoformat()}'"

params_base = {
    "$select": ", ".join(FIELDS),
    "$where":  where,
    "$limit":  LIMIT,
}
headers = {"X-App-Token": TOKEN} if TOKEN else {}

# ---------------------------------------------------------------------------
# Pagination
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
    print("No vacant-violation rows.")
    sys.exit(0)

# ---------------------------------------------------------------------------
# DataFrame & typing
# ---------------------------------------------------------------------------
df = pd.DataFrame(rows)

df["pulled_utc"] = pd.Timestamp.utcnow()

for col in FIELDS:
    if col not in df:
        if col == "date":
            df[col] = pd.NaT
        elif col in {"latitude", "longitude"}:
            df[col] = pd.NA
        else:
            df[col] = pd.NA

# cast
df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
for ncol in ("latitude", "longitude"):
    df[ncol] = pd.to_numeric(df[ncol], errors="coerce")

df["pulled_utc"] = pd.to_datetime(df["pulled_utc"], utc=True)

# ---------------------------------------------------------------------------
# Local debug
# ---------------------------------------------------------------------------
if os.getenv("BUCKET") == "LOCAL":
    out = f"viol_test_{_dt.datetime.utcnow().strftime('%Y-%m-%d')}.csv"
    df.to_csv(out, index=False)
    print(f"Wrote {len(df):,} rows → {out}")
    sys.exit(0)

# ---------------------------------------------------------------------------
# Parquet → S3
# ---------------------------------------------------------------------------
_schema = pa.schema([
    ("case_number",   pa.string()),
    ("date",          pa.date32()),
    ("status",        pa.string()),
    ("code",          pa.string()),
    ("code_section",  pa.string()),
    ("description",   pa.string()),
    ("address",       pa.string()),
    ("latitude",      pa.float64()),
    ("longitude",     pa.float64()),
    ("pulled_utc",    pa.timestamp("us")),
])

table = pa.Table.from_pandas(df, schema=_schema)
buf   = pa.BufferOutputStream()
pq.write_table(table, buf, compression="zstd")

boto3.client("s3", region_name=os.getenv("AWS_REGION")).put_object(
    Bucket=os.getenv("BUCKET"), Key=key, Body=buf.getvalue().to_pybytes())
print(f"Wrote {len(df):,} violation rows → s3://{os.getenv('BUCKET')}/{key}")
