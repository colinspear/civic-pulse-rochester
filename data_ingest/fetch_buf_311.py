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

primary_dt_field = "createddate"
lookback_default = 30
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
    print("No 311 rows."); exit()

df = pd.DataFrame(rows)
df["pulled_utc"] = pd.Timestamp.utcnow()

ymd = datetime.datetime.utcnow().strftime("year=%Y/month=%m/day=%d")
key = f"raw/buf_311/{ymd}/part-0.parquet"

if os.getenv("BUCKET") == "LOCAL":
    out = f'311_test_{datetime.datetime.utcnow().strftime("%Y-%m-%d")}.csv'
    df.to_csv(out, index=False)          # quick sanity file
    print(f"Wrote {len(df):,} rows → {out}")
    sys.exit(0)

buf = pa.BufferOutputStream()
pq.write_table(pa.Table.from_pandas(df), buf, compression="zstd")
boto3.client("s3", region_name=os.getenv("AWS_REGION")).put_object(
        Bucket=os.getenv("BUCKET"), Key=key, Body=buf.getvalue().to_pybytes())
print(f"Wrote {len(df):,} 311 rows → s3://{os.getenv('BUCKET')}/{key}")
