"""
Buffalo Civic‑Pulse – Streamlit front‑end  
• Choropleth of the latest monthly pulse index  
• SHAP bar chart + narrative for clicked tract  
Prereqs  ──────────────────────────────────────────────
  pip install streamlit pandas pydeck boto3 awswrangler shap matplotlib
  export AWS_REGION=us‑east‑1  (and AWS creds w/ read‑only S3/Athena)
  export MAPBOX_TOKEN=<your token>
Run with:  streamlit run pulse_app.py
"""
import os, json, boto3, pandas as pd, geopandas as gpd, streamlit as st, pydeck as pdk, awswrangler as wr, shap
from utils import extract_tract_from_event
from pathlib import Path

# ───────────────────────── AWS + Athena helpers ──
DATABASE = "civic_pulse"
METRICS_VIEW = "civic_pulse.vw_pulse_metrics_latest"
SHAP_VIEW    = "civic_pulse.vw_pulse_shap_latest"

tracts = "../data/erie_tracts.geojson"
# Only allow Buffalo (Erie County, NY) tracts
ERIE_TRACT_PREFIX = "36029"

wr.config.athena_output_location = "s3://civic-pulse-rochester/athena_results/"
@st.cache_data(show_spinner=False)
def load_metrics():
    return wr.athena.read_sql_query(f"SELECT * FROM {METRICS_VIEW}", database=DATABASE)

@st.cache_data(show_spinner=False)
def load_shap():
    df = wr.athena.read_sql_query(f"SELECT * FROM {SHAP_VIEW}", database=DATABASE)
    return df

@st.cache_data(show_spinner=False)
def load_tract_shapes() -> gpd.GeoDataFrame:
    gdf = gpd.read_file(tracts).to_crs(epsg=4326)
    gdf["tract"] = gdf["GEOID"]          # keep a plain str key
    return gdf[["tract", "geometry"]]



tract_shapes = load_tract_shapes()
metrics   = load_metrics()
shap_long = load_shap()

# Restrict all dataframes to Erie County tracts
tract_shapes = tract_shapes[tract_shapes["tract"].astype(str).str.startswith(ERIE_TRACT_PREFIX)]
metrics = metrics[metrics["tract"].astype(str).str.startswith(ERIE_TRACT_PREFIX)]
shap_long = shap_long[shap_long["tract"].astype(str).str.startswith(ERIE_TRACT_PREFIX)]
latest_ts = pd.to_datetime(metrics["30_day_start"].max()).date()

tract_gdf = tract_shapes.merge(metrics, on="tract", how="left").fillna(0)
# -- keep min/max for colour scaling
minScore, maxScore = tract_gdf["score"].min(), tract_gdf["score"].max()
score_norm = "(properties.score - minScore) / (maxScore - minScore)"

# Initialise session state for selected tract
if "selected_tract" not in st.session_state:
    st.session_state.selected_tract = metrics["tract"].iloc[0]

# ───────────────────────── UI layout ─────────────
st.set_page_config(page_title="Buffalo Civic‑Pulse", layout="wide")
st.title("Buffalo Civic‑Pulse")
# st.caption(f"Latest 30‑day window starting {latest_ts:%B %d, %Y}")
st.caption(f"Latest 30‑day window starting {latest_ts}")

left, right = st.columns([2, 1])

# ───────────────────────── Map panel ─────────────
with left:
    st.subheader("Pulse index by census tract")
    # Build GeoJSON from the metrics table (lazily cached on disk)
    geojson_path = Path(tracts)
    if not geojson_path.exists():
        st.error("Missing {tracts} – place it next to this script.")
        st.stop()
    with geojson_path.open() as f:
        geojson = json.load(f)
    # Merge score into Feature properties
    score_map = dict(zip(metrics["tract"], metrics["score"]))
    for feat in geojson["features"]:
        tract_id = feat["properties"]["GEOID"]
        feat["properties"]["score"] = score_map.get(tract_id)
    # PyDeck choropleth
    scoreNorm_expr = "(properties.score - minScore) / (maxScore - minScore)"

    # GeoPandas can't directly serialise Python ``date`` objects.
    # ``default=str`` converts them to ISO formatted strings for JSON output.
    geojson_data = json.loads(tract_gdf.to_json(default=str))
    layer = pdk.Layer(
        "GeoJsonLayer",
        data=geojson_data,
        id="tract-layer",
        pickable=True,
        auto_highlight=True,
        getFillColor=f"[255, 200 * (1 - {score_norm}), 0]",
        getLineColor=[80, 80, 80],
        lineWidthMinPixels=0.5,
        opacity=0.6,
    )

    # normalise score 0‑1 for colour
    smin, smax = metrics["score"].min(), metrics["score"].max()
    scoreNorm_expr = f"(properties.score - {smin}) / ({smax - smin})"  # used in get_fill_color

    view_state = pdk.ViewState(latitude=42.9, longitude=-78.85, zoom=10.5)
    r = pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip={"text": "{GEOID}\nScore: {score}"})
    event = st.pydeck_chart(
        r,
        use_container_width=True,
        on_select="rerun",
        key="map",
    )

    # Update selected tract when a polygon is clicked
    tract_id = extract_tract_from_event(event)
    if tract_id and tract_id in metrics["tract"].values:
        st.session_state.selected_tract = tract_id

# ───────────────────────── Side panel – SHAP / details ──
with right:
    st.subheader("Tract drill‑down")
    clicked = st.text_input("Enter tract GEOID", key="selected_tract")
    if clicked not in metrics["tract"].values:
        st.info("Click a tract on the map or enter an Erie County GEOID")
        st.stop()
    row = metrics.loc[metrics["tract"] == clicked].iloc[0]
    st.metric("Pulse score", f"{row['score']:.2f}")
    st.caption("Component metrics (30‑day totals)")
    # Handle legacy + current column names gracefully
    metric_cols = {
        "Crime/1k pop": ["crime_per_1k", "crime_rate"],
        "Vacant code": ["vacant_code_count", "vacant_code_cnt", "vacant_rate"],
        "Permits": ["permit_count", "permit_cnt", "permit_rate"],
        "Licences": ["licence_count", "licence_cnt", "license_count", "licence_rate"],
        "311 calls": ["calls_count", "calls_cnt", "calls_rate"],
    }
    vals = {}
    for label, candidates in metric_cols.items():
        for c in candidates:
            if c in row:
                vals[label] = row[c]
                break
    st.write(pd.Series(vals))

    st.caption("Feature influence (SHAP)")
    sub = shap_long[shap_long.tract == clicked]
    if sub.empty:
        st.write("SHAP not available – tract lacked data in training window.")
    else:
        import matplotlib.pyplot as plt, numpy as np
        order = sub.groupby("feature")["shap"].mean().abs().sort_values().index
        fig, ax = plt.subplots(figsize=(4,3))
        ax.barh(order, sub.groupby("feature")["shap"].mean().loc[order])
        ax.set_xlabel("Mean |SHAP|")
        st.pyplot(fig, use_container_width=True)

        # Narrative stub
        top = sub.groupby("feature")["shap"].mean().abs().sort_values(ascending=False).head(3)
        narrative = (
            f"In this tract, the pulse score is driven mainly by **{top.index[0]}**, "
            f"followed by **{top.index[1]}** and **{top.index[2]}**."
        )
        st.markdown(narrative)
