import os, datetime, requests, pandas as pd, pyarrow as pa, pyarrow.parquet as pq, boto3

URL = ("https://maps.cityofrochester.gov/arcgis/rest/services/"
       "RPD/RPD_Part_I_Crime/FeatureServer/3/query")
DATE_FIELD = "Reported_Timestamp"          # <- confirmed

def page(offset, since_ms):
    params = {
        "where": "1=1",          # <-- no date filter
        "outFields": "*",
        "returnGeometry": "false",
        "resultRecordCount": 2000,
        "resultOffset": offset,
        "f": "json"
    }

    r = requests.get(URL, params=params, timeout=30)
    r.raise_for_status()
    js = r.json()
    return js.get("features", [])          # <- safe lookup

def main():
    LOOKBACK = int(os.getenv("LOOKBACK_DAYS", "3"))   # default 3
    since_ms = int((datetime.datetime.utcnow()
                - datetime.timedelta(days=LOOKBACK)).timestamp() * 1000)

    rows, off = [], 0
    while True:
        batch = page(off, since_ms)
        if not batch:
            break
        rows.extend(batch)
        off += 2000

    if not rows:
        print("No new crime rows."); return

    df = pd.json_normalize(rows)
    df["pulled_utc"] = pd.Timestamp.utcnow()

    table = pa.Table.from_pandas(df)
    if os.getenv("BUCKET") == "LOCAL":
        pq.write_table(table, "crime_test.parquet", compression="zstd")
        print("Wrote local crime_test.parquet"); return

    ymd = datetime.datetime.utcnow().strftime("%Y/%m/%d")
    key = f"raw/crime/{ymd}/part-0.parquet"

    buf = pa.BufferOutputStream()
    pq.write_table(table, buf, compression="zstd")

    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION"))
    s3.put_object(
        Bucket=os.getenv("BUCKET"),
        Key=key,
        Body=buf.getvalue()
    )

if __name__ == "__main__":
    main()
