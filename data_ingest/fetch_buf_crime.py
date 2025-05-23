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

lookback_default = 1
target = os.getenv("TARGET_DATE")           # optional YYYY-MM-DD

if target:
    target_dt  = pd.to_datetime(target).date()      # -> datetime.date
    since_iso  = target_dt.isoformat()              # '2025-05-08'
    y, m, d    = target_dt.year, f"{target_dt.month:02}", f"{target_dt.day:02}"
    key = f"raw/buf_crime/year={y}/month={m}/day={d}/part-0.parquet"
else:
    lookback   = int(os.getenv("LOOKBACK_DAYS", str(lookback_default)))
    since_dt   = (datetime.datetime.utcnow() - datetime.timedelta(days=lookback)).date()
    since_iso  = since_dt.isoformat()
    ymd        = datetime.datetime.utcnow().strftime("year=%Y/month=%m/day=%d")
    key = f"raw/buf_crime/{ymd}/part-0.parquet"

primary_dt_field = "incident_datetime"

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

for col in FIELDS:
    if col not in df:
        if col in ["incident_datetime", "pulled_utc"]:
            df[col] = pd.NaT                    # timestamp
        elif col in ["latitude", "longitude"]:
            df[col] = pd.NA                     # numeric placeholder
        else:
            df[col] = pd.NA                     # string

# cast timestamps
df["incident_datetime"] = pd.to_datetime(df["incident_datetime"],
                                         utc=True, errors="coerce")
df["pulled_utc"]        = pd.to_datetime(df["pulled_utc"],
                                         utc=True, errors="coerce")

# cast coordinates
df[["latitude", "longitude"]] = df[["latitude", "longitude"]].apply(
    pd.to_numeric, errors="coerce"
)

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
