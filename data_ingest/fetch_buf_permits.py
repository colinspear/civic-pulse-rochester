"""
Pull last week's permits from Buffalo’s Socrata feed 9p2d-f3yt
and write to s3://$BUCKET/raw/buf_permits/year=YYYY/…/part-0.parquet
Env: AWS_REGION  BUCKET
Optional:  SOCRATA_APP_TOKEN  LOOKBACK_DAYS (default 1)
"""

import os, sys, datetime, requests, pandas as pd, pyarrow as pa, pyarrow.parquet as pq, boto3
from utils.geocode import census_batch_geocode

BASE = "https://data.buffalony.gov/resource/9p2d-f3yt.json"
TOKEN = os.getenv("SOCRATA_APP_TOKEN", "")
FIELDS = ["apno","aptype","issued","stname", "city",
          "state", "zip", "latitude", "longitude", "value"]

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
df.rename(columns={"latitude": "latitude_orig", "longitude": "longitude_orig"})
df.reset_index(inplace=True, names='id')

if df.shape[0] < 10000:
    geo = census_batch_geocode(
        df[["id", "stname", "city", "state", "zip"]], 
        id_col="id", 
        addr_col=["stname", "city", "state", "zip"]
        )
else:
    geo = pd.DataFrame(columns=['latitude', 'longitude', 'match_ok'])
    i = 0
    while i < df.shape[0]:
        j = i + 9999
        print(f"Processing rows {i}-{j}")
        batch_df = geocode_df.iloc[i:j]

        try:
            _ = census_batch_geocode(batch_df, id_col="id", addr_col="geo_addr")
            geo = pd.concat([geo, _], ignore_index=True)

        except:
            print(f'  {i}-{j} raised an exception.')
        
        i += 10000

df = df.merge(geo, how="left", on="id", suffixes=["_orig", ""])
geo_cols = ["latitude_orig", "longitude_orig", "latitude", "longitude"]
df[geo_cols] = df[geo_cols].apply(pd.to_numeric, errors="coerce")

if os.getenv("BUCKET") == "LOCAL":
    out = f'permits_test_{datetime.datetime.utcnow().strftime("%Y-%m-%d")}.csv'
    df.to_csv(out, index=False)          # quick sanity file
    print(f"Wrote {len(df):,} rows → {out}")
    sys.exit(0)

schema = pa.schema([
    ("apno",            pa.string()),
    ("aptype",          pa.string()),
    ("issued",          pa.date32()),
    ("stname",          pa.string()),
    ("city",            pa.string()),
    ("state",           pa.string()),
    ("zip",             pa.string()),
    ("value",           pa.float64()),
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
print(f"Wrote {len(df):,} Permit rows → s3://{os.getenv('BUCKET')}/{key}")
