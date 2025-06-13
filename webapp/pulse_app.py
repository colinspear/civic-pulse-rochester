"""
Buffalo Civic‑Pulse – Streamlit front‑end (Plotly edition)
=========================================================
Swaps the previous pydeck map for a Plotly Mapbox choropleth and uses
`streamlit‑plotly‑mapbox‑events` to capture polygon clicks so the sidebar
updates automatically.

Run:
    pip install streamlit plotly streamlit-plotly-mapbox-events geopandas awswrangler shap matplotlib
    streamlit run pulse_app.py
"""
import os, json, boto3, pandas as pd, geopandas as gpd, streamlit as st, pydeck as pdk, awswrangler as wr, shap
from utils import extract_tract_from_event
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np

import awswrangler as wr
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.express as px
import shap
import streamlit as st
from streamlit_plotly_mapbox_events import plotly_mapbox_events

tracts = "../data/erie_tracts.geojson"
# Only allow Buffalo (Erie County, NY) tracts
ERIE_TRACT_PREFIX = "36029"

wr.config.athena_output_location = ATHENA_OUT

# ── Data loaders --------------------------------------------------------
@st.cache_data(show_spinner=False)
def load_metrics():
    return wr.athena.read_sql_query(f"SELECT * FROM {METRICS_SQL}", database=DB)

@st.cache_data(show_spinner=False)
def load_shap():
    return wr.athena.read_sql_query(f"SELECT * FROM {SHAP_SQL}", database=DB)

@st.cache_data(show_spinner=False)
def load_geo(tract_path: Path):
    if not tract_path.exists():
        st.error(f"Missing {tract_path} – place it next to this script.")
        st.stop()
    gdf = gpd.read_file(tract_path).to_crs(epsg=4326)
    gdf["tract"] = gdf["GEOID"]
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
min_score, max_score = tract_gdf["score"].min(), tract_gdf["score"].max()

# Initialise session state for selected tract
if "selected_tract" not in st.session_state:
    st.session_state.selected_tract = metrics["tract"].iloc[0]

# ───────────────────────── UI layout ─────────────
st.set_page_config(page_title="Buffalo Civic‑Pulse", layout="wide")
st.title("Buffalo Civic‑Pulse")
st.caption(f"Latest 30‑day window starting {latest_ts}")
left, right = st.columns([2, 1])

# ── Map panel (Plotly) --------------------------------------------------
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
    # Normalise score 0‑1 for colour
    score_norm = f"(properties.score - {min_score}) / ({max_score - min_score})"

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
        height=650,
    )

    view_state = pdk.ViewState(latitude=42.9, longitude=-78.85, zoom=10.5)
    r = pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip={"text": "{GEOID}\nScore: {score}"})
    event = st.pydeck_chart(
        r,
        use_container_width=True,
        on_select="rerun",
        key="map",
    )

    st.caption("Redder tracts have higher pulse scores (more distress)")

    # Update selected tract when a polygon is clicked
    tract_id = extract_tract_from_event(event)
    if tract_id and tract_id in metrics["tract"].values:
        st.session_state.selected_tract = tract_id

    if clicks:
        tract_id = clicks[0].get("location")  # GEOID
        if tract_id:
            st.session_state["selected_tract"] = tract_id

# ── Drill‑down panel ----------------------------------------------------
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
    sub = shap_long[shap_long["tract"] == clicked]
    if sub.empty:
        st.write("SHAP not available – tract lacked data in training window.")
    else:
        order = sub.groupby("feature")["shap"].mean().abs().sort_values().index
        fig2, ax2 = plt.subplots(figsize=(4, 3))
        ax2.barh(order, sub.groupby("feature")["shap"].mean().loc[order])
        ax2.set_xlabel("Mean |SHAP|")
        st.pyplot(fig2, use_container_width=True)

        top = (
            sub.groupby("feature")["shap"].mean().abs().sort_values(ascending=False).head(3)
        )
        st.markdown(
            f"Pulse score here is driven mainly by **{top.index[0]}**, followed by **{top.index[1]}** and **{top.index[2]}**."
        )
