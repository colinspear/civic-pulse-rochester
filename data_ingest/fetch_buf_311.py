"""
Pull yesterday’s incidents from Buffalo’s Socrata feed 3tj7-3tdz
and write to s3://$BUCKET/raw/buf_311/year=YYYY/…/part-0.parquet
Env: AWS_REGION  BUCKET
Optional:  SOCRATA_APP_TOKEN  LOOKBACK_DAYS (default 1)
"""

import os, sys, datetime, requests, pandas as pd, pyarrow as pa, pyarrow.parquet as pq, boto3

BASE = "https://data.buffalony.gov/resource/3tj7-3tdz.json"
TOKEN = os.getenv("SOCRATA_APP_TOKEN", "")
FIELDS = ['createddate', 'casenumber', "department", "division", "type", "status", 
          "statusdescription", "closeddate", "latitude", "longitude"]

target = os.getenv("TARGET_DATE")           # fmt YYYY-MM-DD, optional

if target:
    target_dt = pd.to_datetime(target).date()
    since_iso = target_dt.isoformat()       # API filter for that one day
    y,m,d = target_dt.year, f"{target_dt.month:02}", f"{target_dt.day:02}"
    key = f"raw/buf_311/year={y}/month={m}/day={d}/part-0.parquet"
else:
    # default: yesterday look-back, run-date partition
    lookback_default = 1
    lookback = int(os.getenv("LOOKBACK_DAYS", str(lookback_default)))
    since_iso = (datetime.datetime.utcnow() - datetime.timedelta(days=lookback)).isoformat()
    ymd = datetime.datetime.utcnow().strftime("year=%Y/month=%m/day=%d")
    key = f"raw/buf_311/{ymd}/part-0.parquet"

primary_dt_field = "createddate"
params = {
    "$select": ", ".join(FIELDS),
    "$limit": 50000,
    "$where": f"{primary_dt_field} >= '{since_iso}'"
}
hdrs = {"X-App-Token": TOKEN} if TOKEN else {}

rows = requests.get(BASE, params=params, headers=hdrs, timeout=60).json()
if not rows:
    print("No 311 rows."); exit()

df = pd.DataFrame(rows)
df["pulled_utc"] = pd.Timestamp.utcnow()

for col in FIELDS:
    if col not in df:
        if col in ["createddate", "closeddate", "pulled_utc"]:
            df[col] = pd.NaT                       # datetime column
        elif col in ["latitude", "longitude"]:
            df[col] = pd.NA                        # numeric
        else:
            df[col] = pd.NA                        # string

df["createddate"] = pd.to_datetime(df["createddate"], utc=True, errors="coerce")
df["closeddate"]  = pd.to_datetime(df["closeddate"],  utc=True, errors="coerce")
df["pulled_utc"]  = pd.to_datetime(df["pulled_utc"],  utc=True, errors="coerce")

df[["latitude", "longitude"]] = df[["latitude", "longitude"]].apply(
    pd.to_numeric, errors="coerce"
)

if os.getenv("BUCKET") == "LOCAL":
    out = f'311_test_{datetime.datetime.utcnow().strftime("%Y-%m-%d")}.csv'
    df.to_csv(out, index=False)          # quick sanity file
    print(f"Wrote {len(df):,} rows → {out}")
    sys.exit(0)

schema = pa.schema([
    ("createddate",         pa.timestamp("us")),
    ("casenumber",          pa.string()),
    ("department",          pa.string()),
    ("division",            pa.string()),
    ("type",                pa.string()),
    ("status",              pa.string()),
    ("statusdescription",   pa.string()),
    ("closeddate",          pa.timestamp("us")),
    ("latitude",            pa.float64()),
    ("longitude",           pa.float64()),
    ("pulled_utc",          pa.timestamp("us"))
])

table = pa.Table.from_pandas(df, schema=schema)
buf   = pa.BufferOutputStream()
pq.write_table(table, buf, compression="zstd")
boto3.client("s3", region_name=os.getenv("AWS_REGION")).put_object(
        Bucket=os.getenv("BUCKET"), Key=key, Body=buf.getvalue().to_pybytes())
print(f"Wrote {len(df):,} 311 rows → s3://{os.getenv('BUCKET')}/{key}")
