"""
Pull last 30 day's business licenses from Buffalo’s Socrata feed qcyy-feh8
and write to s3://$BUCKET/raw/buf_biz/year=YYYY/…/part-0.parquet
Env: AWS_REGION  BUCKET
Optional:  SOCRATA_APP_TOKEN  LOOKBACK_DAYS (default 30)
"""

from __future__ import annotations
import requests, pandas as pd
import os, sys, datetime, pyarrow as pa, pyarrow.parquet as pq, boto3
from utils.geocode import census_batch_geocode


BASE = "https://data.buffalony.gov/resource/qcyy-feh8.json"
TOKEN = os.getenv("SOCRATA_APP_TOKEN", "")
FIELDS = ["uniqkey","code","descript","licstatus",
          "statusdttm","licensedttm","issdttm",
          "address", "city", "state", "zip", 
          "latitude","longitude"]

target = os.getenv("TARGET_DATE")           # fmt YYYY-MM-DD, optional
lookback_default = 1

if target:
    target_dt = pd.to_datetime(target).date()
    since_iso = target_dt.isoformat()       # API filter for that one day
    y,m,d = target_dt.year, f"{target_dt.month:02}", f"{target_dt.day:02}"
    key = f"raw/buf_biz/year={y}/month={m}/day={d}/part-0.parquet"
else:
    lookback = int(os.getenv("LOOKBACK_DAYS", str(lookback_default)))
    since_iso = (datetime.datetime.utcnow() - datetime.timedelta(days=lookback)).isoformat()
    ymd = datetime.datetime.utcnow().strftime("year=%Y/month=%m/day=%d")
    key = f"raw/buf_biz/{ymd}/part-0.parquet"

primary_dt_field = "issdttm"
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
    print("No Business License rows."); exit()

df = pd.DataFrame(rows)
df["pulled_utc"] = pd.Timestamp.utcnow()

for col in FIELDS:
    if col not in df:
        if col in ["statusdttm", "licenseddttm", "issdttm", "pulled_utc"]:
            df[col] = pd.NaT
        elif col in ["latitude", "longitude"]:
            df[col] = pd.NA
        else:
            df[col] = pd.NA

df["issdttm"]    = pd.to_datetime(df["issdttm"],    utc=True, errors="coerce")
df["statusdttm"] = pd.to_datetime(df["statusdttm"], utc=True, errors="coerce")
df["licensedttm"] = pd.to_datetime(df["statusdttm"], utc=True, errors="coerce")
df["pulled_utc"] = pd.to_datetime(df["pulled_utc"], utc=True, errors="coerce")
df.rename(columns={"latitude": "latitude_orig", "longitude": "longitude_orig"})
df.reset_index(inplace=True, names='id')

geo_df = census_batch_geocode(
        df[["id", "address", "city", "state", "zip"]], 
        id_col="id", 
        addr_col=["address", "city", "state", "zip"]
        )

df = df.merge(geo_df, how="left", on="id", suffixes=["_orig", ""])
geo_cols = ["latitude_orig", "longitude_orig", "latitude", "longitude"]
df[geo_cols] = df[geo_cols].apply(pd.to_numeric, errors="coerce")

if os.getenv("BUCKET") == "LOCAL":
    out = f'biz_test_{datetime.datetime.utcnow().strftime("%Y-%m-%d")}.csv'
    df.to_csv(out, index=False)          # quick sanity file
    print(f"Wrote {len(df):,} rows → {out}")
    sys.exit(0)

schema = pa.schema([
    ("uniqkey",         pa.string()),
    ("code",            pa.string()),
    ("descript",        pa.string()),
    ("licstatus",       pa.string()),
    ("statusdttm",      pa.timestamp("us")),
    ("licensedttm",     pa.timestamp("us")),
    ("issdttm",         pa.timestamp("us")),
    ("address",         pa.string()),
    ("city",            pa.string()),
    ("state",           pa.string()),
    ("zip",             pa.string()),
    ("latitude_orig",   pa.float64()),
    ("longitude_orig",  pa.float64()),
    ("latitude",        pa.float64()),
    ("longitude",       pa.float64()),
    ("match_ok",        pa.bool_()),
    ("pulled_utc",      pa.timestamp("us"))
])

table = pa.Table.from_pandas(df, schema=schema)
buf   = pa.BufferOutputStream()
pq.write_table(table, buf, compression="zstd")
boto3.client("s3", region_name=os.getenv("AWS_REGION")).put_object(
        Bucket=os.getenv("BUCKET"), Key=key, Body=buf.getvalue().to_pybytes())
print(f"Wrote {len(df):,} Business License rows → s3://{os.getenv('BUCKET')}/{key}")
