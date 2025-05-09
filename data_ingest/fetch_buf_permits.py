"""
Pull last week's permits from Buffalo’s Socrata feed 9p2d-f3yt
and write to s3://$BUCKET/raw/buf_permits/year=YYYY/…/part-0.parquet
Env: AWS_REGION  BUCKET
Optional:  SOCRATA_APP_TOKEN  LOOKBACK_DAYS (default 7)
"""

import os, sys, datetime, requests, pandas as pd, pyarrow as pa, pyarrow.parquet as pq, boto3

BASE = "https://data.buffalony.gov/resource/9p2d-f3yt.json"
TOKEN = os.getenv("SOCRATA_APP_TOKEN", "")
FIELDS = ["apno","aptype","issued","stname","value"]

lookback_default = 1
target = os.getenv("TARGET_DATE")

if target:
    target_dt  = pd.to_datetime(target).date()
    since_iso  = target_dt.isoformat()
    y, m, d    = target_dt.year, f"{target_dt.month:02}", f"{target_dt.day:02}"
    key = f"raw/buf_permits/year={y}/month={m}/day={d}/part-0.parquet"
else:
    lookback   = int(os.getenv("LOOKBACK_DAYS", str(lookback_default)))
    since_dt   = (datetime.datetime.utcnow() - datetime.timedelta(days=lookback)).date()
    since_iso  = since_dt.isoformat()
    ymd        = datetime.datetime.utcnow().strftime("year=%Y/month=%m/day=%d")
    key = f"raw/buf_permits/{ymd}/part-0.parquet"

primary_dt_field = "issued"

params = {
    "$select": ", ".join(FIELDS),
    "$limit": 50000,
    "$where": f"{primary_dt_field} >= '{since_iso}'"
}

hdrs = {"X-App-Token": TOKEN} if TOKEN else {}

rows = requests.get(BASE, params=params, headers=hdrs, timeout=60).json()
if not rows:
    print("No Permit rows."); exit()

df = pd.DataFrame(rows)
df["pulled_utc"] = pd.Timestamp.utcnow()

for col in FIELDS:
    if col not in df:
        if col in ["issued", "pulled_utc"]:
            df[col] = pd.NaT
        elif col == "value":
            df[col] = pd.NA
        else:
            df[col] = pd.NA

df["issued"]     = pd.to_datetime(df["issued"], errors="coerce").dt.date
df["pulled_utc"] = pd.to_datetime(df["pulled_utc"], utc=True, errors="coerce")

df["value"] = pd.to_numeric(df["value"], errors="coerce")

if os.getenv("BUCKET") == "LOCAL":
    out = f'permits_test_{datetime.datetime.utcnow().strftime("%Y-%m-%d")}.csv'
    df.to_csv(out, index=False)          # quick sanity file
    print(f"Wrote {len(df):,} rows → {out}")
    sys.exit(0)

schema = pa.schema([
    ("apno",        pa.string()),
    ("aptype",      pa.string()),
    ("issued",      pa.date32()),
    ("stname",      pa.string()),
    ("value",       pa.float64()),
    ("pulled_utc",  pa.timestamp("us"))
])

table = pa.Table.from_pandas(df, schema=schema)
buf   = pa.BufferOutputStream()
pq.write_table(table, buf, compression="zstd")
boto3.client("s3", region_name=os.getenv("AWS_REGION")).put_object(
        Bucket=os.getenv("BUCKET"), Key=key, Body=buf.getvalue().to_pybytes())
print(f"Wrote {len(df):,} Permit rows → s3://{os.getenv('BUCKET')}/{key}")
