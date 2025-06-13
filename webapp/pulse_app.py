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

from __future__ import annotations

import json
import os
from pathlib import Path

import awswrangler as wr
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.express as px
import shap
import streamlit as st
from streamlit_plotly_mapbox_events import plotly_mapbox_events

# ── Config -------------------------------------------------------------
DB          = "civic_pulse"
METRICS_SQL = "civic_pulse.vw_pulse_metrics_latest"
SHAP_SQL    = "civic_pulse.vw_pulse_shap_latest"
ATHENA_OUT  = "s3://civic-pulse-rochester/athena_results/"
TRACTS_FILE = Path(__file__).with_name("erie_tracts.geojson")

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

@st.cache_data(show_spinner=False)
def build_geojson(_shape_gdf: gpd.GeoDataFrame):
    """Return pure GeoJSON dict (no score added – Plotly uses locations).
    Cast any dates to str to keep JSON serialisable.
    """
    gdf = _shape_gdf.copy()
    for col in gdf.columns:
        if pd.api.types.is_datetime64_any_dtype(gdf[col]):
            gdf[col] = gdf[col].dt.strftime("%Y-%m-%d")
    return json.loads(gdf.to_json())

# ── Load data -----------------------------------------------------------
metrics   = load_metrics()
shap_long = load_shap()
tract_gdf = load_geo(TRACTS_FILE)
geojson   = build_geojson(tract_gdf)
latest_ts = pd.to_datetime(metrics["30_day_start"].max()).date()

# ── Streamlit UI --------------------------------------------------------
st.set_page_config(page_title="Buffalo Civic‑Pulse", layout="wide")
st.title("Buffalo Civic‑Pulse")
st.caption(f"Latest 30‑day window starting {latest_ts}")
left, right = st.columns([2, 1])

# ── Map panel (Plotly) --------------------------------------------------
with left:
    st.subheader("Pulse index by census tract")

    fig = px.choropleth_mapbox(
        metrics,
        geojson=geojson,
        locations="tract",
        color="score",
        color_continuous_scale="YlOrRd",
        range_color=(metrics.score.min(), metrics.score.max()),
        featureidkey="properties.tract",   # <-- tell Plotly where the GEOID lives
        mapbox_style="carto-positron",
        zoom=9.8,
        center={"lat": 42.9, "lon": -78.85},
        opacity=0.6,
        height=650,
    )

    fig.update_layout(margin=dict(r=0, t=0, l=0, b=0))
    fig.update_coloraxes(colorbar_title="Pulse score")

    # Capture clicks
    clicks, *_ = plotly_mapbox_events(
        fig,
        click_event=True,
        select_event=False,
        hover_event=False,
        override_height=650,
        key="pulse_map",
    )

    if clicks:
        tract_id = clicks[0].get("location")  # GEOID
        if tract_id:
            st.session_state["selected_tract"] = tract_id

# ── Drill‑down panel ----------------------------------------------------
with right:
    st.subheader("Tract drill‑down")

    default_tract = st.session_state.get("selected_tract",
                                         metrics["tract"].iloc[0])
    clicked = st.text_input("Enter tract GEOID", value=default_tract)

    if clicked not in metrics["tract"].values:
        st.info("Click a tract on the map or enter a valid GEOID.")
        st.stop()

    row = metrics.loc[metrics["tract"] == clicked].iloc[0]
    st.metric("Pulse score", f"{row['score']:.2f}")

    cols = [
        "crime_rate",
        "vacant_code_cnt",
        "permit_cnt",
        "licence_cnt",
        "calls_cnt",
    ]
    labels = {
        "crime_rate": "Crime /1k pop",
        "vacant_code_cnt": "Vacant codes",
        "permit_cnt": "Permits",
        "licence_cnt": "Biz licences",
        "calls_cnt": "311 calls",
    }
    st.caption("30‑day component totals")
    st.write(row[cols].rename(index=labels))

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
