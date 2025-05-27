import awswrangler as wr, pyarrow.compute as pc, s3fs

fs = s3fs.S3FileSystem()
paths = fs.glob("s3://<BUCKET>/raw/buf_311/year=*/month=*/day=*/part-0.parquet")

for p in paths:
    tbl = wr.s3.read_parquet(p)
    lat, lon = tbl["latitude"], tbl["longitude"]
    flip = (
        pc.and_(lat < -70, lat > -80) &
        pc.and_(lon > 40,  lon < 45)
    )
    if flip.any():
        tbl = tbl.set_column(tbl.schema.get_field_index("latitude"), "latitude",
                             pc.if_else(flip, lon, lat))
        tbl = tbl.set_column(tbl.schema.get_field_index("longitude"), "longitude",
                             pc.if_else(flip, lat, lon))
        wr.s3.to_parquet(tbl, p, mode="overwrite")
        print("repaired", p)
