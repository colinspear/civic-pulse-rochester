from __future__ import annotations
import io, csv, requests, pandas as pd

CENSUS_BATCH_URL = (
    "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"
)  # returntype=locations implicit
BENCHMARK = "Public_AR_Current"            # same one you used before
MAX_BATCH = 10_000                         # hard API limit

def census_batch_geocode(df: pd.DataFrame,
                         id_col: str,
                         addr_col: str | list[str]) -> pd.DataFrame:
    """
    Batch-geocode ≤10,000 rows. Returns a DataFrame with:
        [id_col, 'latitude', 'longitude', 'match_ok']
    Unmatched rows have NaNs for lat/lon.

    Parameters
    ----------
    df        : DataFrame with one row per address
    id_col    : Unique ID column name
    addr_col  : Either a single column name or list of column names to send
                (must match Census batch geocoder's expected format: 
                 [ID, street, city, state, ZIP])
    """
    if len(df) > MAX_BATCH:
        raise ValueError(f"Batch limit is {MAX_BATCH}; got {len(df)}")

    # Ensure addr_col is list
    if isinstance(addr_col, str):
        addr_col = [addr_col]

    # Ensure exactly 5 columns total (ID + up to 4 address fields)
    required_cols = [id_col] + addr_col
    if not all(col in df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df.columns]
        raise ValueError(f"Missing required columns: {missing}")

    # ---- 1. build CSV in-memory ------------------------------------------
    buff = io.StringIO()
    w = csv.writer(buff, lineterminator="\n")
    for _, row in df[required_cols].iterrows():
        record = [row[id_col]] + [row[col] if pd.notnull(row[col]) else "" for col in addr_col]
        # Pad to ensure total of 5 columns (API expects it)
        record += [""] * (5 - len(record))
        w.writerow(record)
    buff.seek(0)

    with open("debug_census_payload.csv", "w", encoding="utf-8") as f:
        f.write(buff.getvalue())

    # ---- 2. POST to Census API -------------------------------------------
    files = {"addressFile": ("addrs.csv", buff.getvalue())}
    data  = {"benchmark": BENCHMARK}
    resp  = requests.post(CENSUS_BATCH_URL, files=files, data=data, timeout=60)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        print("STATUS:", resp.status_code)
        print("RESPONSE TEXT:", resp.text[:500])  # first 500 chars
        raise

    # ---- 3. parse returned CSV -------------------------------------------
    out = pd.read_csv(io.StringIO(resp.text), header=None,
                      names=["id", "input_addr", "match", "type",
                             "matched_addr", "coordinates", "tiger", "side"])
    out["latitude"]  = out["coordinates"].str.split(',').str[1].astype(float)
    out["longitude"] = out["coordinates"].str.split(',').str[0].astype(float)
    out["match_ok"]  = out["match"].eq("Match")

    return out[[ "id", "latitude", "longitude", "match_ok" ]]
