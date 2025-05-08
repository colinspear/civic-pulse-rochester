"""
Pull yesterday’s VACANT-building violations from ivrf-k9vm
Env: AWS_REGION  BUCKET     (optional) SOCRATA_APP_TOKEN  LOOKBACK_DAYS
"""

import os, sys, datetime, requests, pandas as pd, pyarrow as pa, pyarrow.parquet as pq, boto3

BASE = "https://data.buffalony.gov/resource/ivrf-k9vm.json"
TOKEN = os.getenv("SOCRATA_APP_TOKEN", "")
FIELDS = ["case_number", "date", "status", "code", "code_section", "description", 
          "address", "latitude", "longitude"]

target = os.getenv("TARGET_DATE")           # fmt YYYY-MM-DD, optional

if target:
    target_dt = pd.to_datetime(target).date()
    since_iso = target_dt.isoformat()       # API filter for that one day
    y,m,d = target_dt.year, f"{target_dt.month:02}", f"{target_dt.day:02}"
    key = f"raw/buf_viol/year={y}/month={m}/day={d}/part-0.parquet"
else:
    # default: yesterday look-back, run-date partition
    lookback_default = 1
    lookback = int(os.getenv("LOOKBACK_DAYS", str(lookback_default)))
    since_iso = (datetime.datetime.utcnow() - datetime.timedelta(days=lookback)).isoformat()
    ymd = datetime.datetime.utcnow().strftime("year=%Y/month=%m/day=%d")
    key = f"raw/buf_viol/{ymd}/part-0.parquet"

primary_dt_field = "date"
lookback_default = 1
lookback = int(os.getenv("LOOKBACK_DAYS", str(lookback_default)))

since = (datetime.datetime.utcnow() - datetime.timedelta(days=lookback)).date()
since_iso = since.isoformat()

params = {
    "$select": ", ".join(FIELDS),
    "$limit": 50000,
    "$where": (
        f"{primary_dt_field} >= '{since_iso}' "
        "AND upper(description) like 'VACANT%'"
    )
}

hdrs = {"X-App-Token": TOKEN} if TOKEN else {}
rows = requests.get(BASE, params=params, headers=hdrs, timeout=60).json()
if not rows:
    print("No vacant-violation rows."); exit()

df = pd.DataFrame(rows)
df["pulled_utc"] = pd.Timestamp.utcnow()
df[primary_dt_field] = pd.to_datetime(df[primary_dt_field], errors="coerce").dt.date

ymd = datetime.datetime.utcnow().strftime("year=%Y/month=%m/day=%d")
key = f"raw/buf_viol/{ymd}/part-0.parquet"

if os.getenv("BUCKET") == "LOCAL":
    out = f'violation_test_{datetime.datetime.utcnow().strftime("%Y-%m-%d")}.csv'
    df.to_csv(out, index=False)
    print(f"Wrote {len(df):,} rows → {out}")
    sys.exit(0)

schema = pa.schema([
    ("casenumber",      pa.string()),
    ("date",            pa.date32()),
    ("status",          pa.string()),
    ("code",            pa.string()),
    ("code_section",    pa.string()),
    ("description",     pa.string()),
    ("address",         pa.string()),
    ("latitude",        pa.float64()),
    ("longitude",       pa.float64()),
    ("pulled_utc",      pa.timestamp("us"))
])

table = pa.Table.from_pandas(df, schema=schema)
buf   = pa.BufferOutputStream()
pq.write_table(table, buf, compression="zstd")
boto3.client("s3", region_name=os.getenv("AWS_REGION")).put_object(
        Bucket=os.getenv("BUCKET"), Key=key, Body=buf.getvalue().to_pybytes())
print(f"Wrote {len(df):,} violation rows → s3://{os.getenv('BUCKET')}/{key}")
