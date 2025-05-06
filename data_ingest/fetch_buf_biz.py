"""
Pull last 30 day's business licenses from Buffalo’s Socrata feed qcyy-feh8
and write to s3://$BUCKET/raw/buf_biz/year=YYYY/…/part-0.parquet
Env: AWS_REGION  BUCKET
Optional:  SOCRATA_APP_TOKEN  LOOKBACK_DAYS (default 30)
"""

import os, sys, datetime, requests, pandas as pd, pyarrow as pa, pyarrow.parquet as pq, boto3

BASE = "https://data.buffalony.gov/resource/qcyy-feh8.json"
TOKEN = os.getenv("SOCRATA_APP_TOKEN", "")
FIELDS = ["uniqkey","code","descript","licstatus",
          "statusdttm","licensedttm","issdttm","latitude","longitude"]

primary_dt_field = "issdttm"
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
    print("No Business License rows."); exit()

df = pd.DataFrame(rows)
df["pulled_utc"] = pd.Timestamp.utcnow()

ymd = datetime.datetime.utcnow().strftime("year=%Y/month=%m/day=%d")
key = f"raw/buf_biz/{ymd}/part-0.parquet"

if os.getenv("BUCKET") == "LOCAL":
    out = f'biz_test_{datetime.datetime.utcnow().strftime("%Y-%m-%d")}.csv'
    df.to_csv(out, index=False)          # quick sanity file
    print(f"Wrote {len(df):,} rows → {out}")
    sys.exit(0)

buf = pa.BufferOutputStream()
pq.write_table(pa.Table.from_pandas(df), buf, compression="zstd")
boto3.client("s3", region_name=os.getenv("AWS_REGION")).put_object(
        Bucket=os.getenv("BUCKET"), Key=key, Body=buf.getvalue().to_pybytes())
print(f"Wrote {len(df):,} Business License rows → s3://{os.getenv('BUCKET')}/{key}")
