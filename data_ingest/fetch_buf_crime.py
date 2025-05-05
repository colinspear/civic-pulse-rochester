"""
Pull yesterday’s incidents from Buffalo’s Socrata feed d6g9-xbgu
and write to s3://$BUCKET/raw/buf_crime/year=YYYY/…/part-0.parquet
Env: AWS_REGION  BUCKET
Optional:  SOCRATA_APP_TOKEN  LOOKBACK_DAYS (default 1)
"""

import os, datetime, requests, pandas as pd, pyarrow as pa, pyarrow.parquet as pq, boto3

BASE = "https://data.buffalony.gov/resource/d6g9-xbgu.json"
TOKEN = os.getenv("SOCRATA_APP_TOKEN", "")

lookback = int(os.getenv("LOOKBACK_DAYS", "1"))
since_iso = (datetime.datetime.utcnow() - datetime.timedelta(days=lookback)).isoformat()

params = {
    "$limit": 50000,
    "$where": f"incident_datetime >= '{since_iso}'"
}
hdrs = {"X-App-Token": TOKEN} if TOKEN else {}

rows = requests.get(BASE, params=params, headers=hdrs, timeout=60).json()
if not rows:
    print("No crime rows."); exit()

df = pd.DataFrame(rows)
df["pulled_utc"] = pd.Timestamp.utcnow()

ymd = datetime.datetime.utcnow().strftime("year=%Y/month=%m/day=%d")
key = f"raw/buf_crime/{ymd}/part-0.parquet"

buf = pa.BufferOutputStream()
pq.write_table(pa.Table.from_pandas(df), buf, compression="zstd")
boto3.client("s3", region_name=os.getenv("AWS_REGION")).put_object(
        Bucket=os.getenv("BUCKET"), Key=key, Body=buf.getvalue().to_pybytes())
print(f"Wrote {len(df):,} crime rows → s3://{os.getenv('BUCKET')}/{key}")
