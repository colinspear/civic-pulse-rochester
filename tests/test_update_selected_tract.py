import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

# Mock heavy dependencies before importing the app
import types
for mod in ["boto3", "pandas", "pydeck", "shap"]:
    if mod not in sys.modules:
        sys.modules[mod] = types.ModuleType(mod)

# Minimal geopandas stub with GeoDataFrame class
gpd = types.ModuleType("geopandas")
class GeoDataFrame:
    def to_crs(self, *args, **kwargs):
        return self
gpd.GeoDataFrame = GeoDataFrame
def read_file(*args, **kwargs):
    return GeoDataFrame()
gpd.read_file = read_file
sys.modules.setdefault("geopandas", gpd)

# Provide a minimal awswrangler stub with config attribute
wr = types.ModuleType("awswrangler")
wr.config = types.SimpleNamespace(athena_output_location=None)
sys.modules.setdefault("awswrangler", wr)

import streamlit as st  # re-import after mocking
from webapp.utils import extract_tract_from_event

def test_extract_tract_from_event_updates_state():
    st.session_state.clear()
    st.session_state.selected_tract = '123'
    event = {
        'selection': {
            'indices': {'tract-layer': [0]},
            'objects': {'tract-layer': [{'properties': {'tract': '456'}}]},
        }
    }
    result = extract_tract_from_event(event)
    assert result == '456'
    if result:
        st.session_state.selected_tract = result
    assert st.session_state.selected_tract == '456'


def test_extract_tract_from_event_no_selection():
    st.session_state.clear()
    st.session_state.selected_tract = '789'
    event = {'selection': {'indices': {}, 'objects': {}}}
    result = extract_tract_from_event(event)
    assert result is None
    assert st.session_state.selected_tract == '789'

