"""
Pull yesterday’s incidents from Buffalo’s Socrata feed d6g9-xbgu
and write to s3://$BUCKET/raw/buf_crime/year=YYYY/…/part-0.parquet
Env: AWS_REGION  BUCKET
Optional:  SOCRATA_APP_TOKEN  LOOKBACK_DAYS (default 1)
"""

import os, sys, datetime, requests, pandas as pd, pyarrow as pa, pyarrow.parquet as pq, boto3

BASE = "https://data.buffalony.gov/resource/d6g9-xbgu.json"
TOKEN = os.getenv("SOCRATA_APP_TOKEN", "")
FIELDS = ['case_number', "incident_datetime", "incident_type_primary",
           "parent_incident_type", "latitude", "longitude"]

target = os.getenv("TARGET_DATE")           # fmt YYYY-MM-DD, optional

if target:
    target_dt = pd.to_datetime(target).date()
    since_iso = target_dt.isoformat()       # API filter for that one day
    y,m,d = target_dt.year, f"{target_dt.month:02}", f"{target_dt.day:02}"
    key = f"raw/buf_crime/year={y}/month={m}/day={d}/part-0.parquet"
else:
    # default: yesterday look-back, run-date partition
    lookback_default = 1
    lookback = int(os.getenv("LOOKBACK_DAYS", str(lookback_default)))
    since_iso = (datetime.datetime.utcnow() - datetime.timedelta(days=lookback)).isoformat()
    ymd = datetime.datetime.utcnow().strftime("year=%Y/month=%m/day=%d")
    key = f"raw/buf_crime/{ymd}/part-0.parquet"

primary_dt_field = "incident_datetime"
lookback_default = 1
lookback = int(os.getenv("LOOKBACK_DAYS", str(lookback_default)))
since_iso = (datetime.datetime.utcnow() - datetime.timedelta(days=lookback)).isoformat()

params = {
    "$select": ", ".join(FIELDS),
    "$limit": 50000,
    "$where": f"{primary_dt_field} >= '{since_iso}'"
}
hdrs = {"X-App-Token": TOKEN} if TOKEN else {}

rows = requests.get(BASE, params=params, headers=hdrs, timeout=60).json()
if not rows:
    print("No crime rows."); exit()

df = pd.DataFrame(rows)
df["pulled_utc"] = pd.Timestamp.utcnow()
df[primary_dt_field] = pd.to_datetime(df[primary_dt_field], utc=True, errors="coerce")


ymd = datetime.datetime.utcnow().strftime("year=%Y/month=%m/day=%d")
key = f"raw/buf_crime/{ymd}/part-0.parquet"

if os.getenv("BUCKET") == "LOCAL":
    out = f'crime_test_{datetime.datetime.utcnow().strftime("%Y-%m-%d")}.csv'
    df.to_csv(out, index=False)          # quick sanity file
    print(f"Wrote {len(df):,} rows → {out}")
    sys.exit(0)

schema = pa.schema([
    ("case_number",            pa.string()),
    ("incident_datetime",      pa.timestamp("us")),
    ("incident_type_primary",  pa.string()),
    ("parent_incident_type",   pa.string()),
    ("latitude",               pa.float64()),
    ("longitude",              pa.float64()),
    ("pulled_utc",             pa.timestamp("us"))
])

table = pa.Table.from_pandas(df, schema=schema)
buf   = pa.BufferOutputStream()
pq.write_table(table, buf, compression="zstd")
boto3.client("s3", region_name=os.getenv("AWS_REGION")).put_object(
        Bucket=os.getenv("BUCKET"), Key=key, Body=buf.getvalue().to_pybytes())
print(f"Wrote {len(df):,} crime rows → s3://{os.getenv('BUCKET')}/{key}")
