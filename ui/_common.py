"""ui/_common.py — shared helpers used across pages (dataset loading, mode badges).
Every dataset (upload OR demo pick) is persisted via storage.manager on first load —
this is the Storage Manager from the roadmap: nothing goes straight from the upload
widget to Databricks. It lands in local file storage + the SQLite registry first, so
reruns don't require re-uploading and every dataset has real history."""
from __future__ import annotations

import io

import pandas as pd
import streamlit as st

from config.demo import DEMO_DATASETS
from config.settings import current_mode, load_settings
from storage import manager as storage_manager


def get_mode() -> str:
    return current_mode(load_settings())


def load_uploaded_file(uploaded_file) -> pd.DataFrame:
    name = uploaded_file.name.lower()
    data = uploaded_file.read()
    buf = io.BytesIO(data)
    if name.endswith(".csv"):
        return pd.read_csv(buf)
    if name.endswith((".xlsx", ".xls")):
        return pd.read_excel(buf)
    if name.endswith(".json"):
        return pd.read_json(buf)
    if name.endswith(".parquet"):
        return pd.read_parquet(buf)
    if name.endswith(".tsv"):
        return pd.read_csv(buf, sep="\t")
    if name.endswith(".txt"):
        return pd.read_csv(buf, sep=None, engine="python")
    raise ValueError(f"Unsupported file type: {uploaded_file.name}")


def dataset_picker(key_prefix: str = "picker"):
    """Renders a dataset source selector (demo dataset OR file upload). Returns
    (dataset_name, dataframe) or (None, None) if nothing selected yet. The resolved
    storage.manager dataset_id (for the Databricks Job path / rerun-without-reupload)
    is stashed in st.session_state[f"{key_prefix}_storage_id"] — callers that need it
    read it from there rather than this function's return signature changing."""
    source = st.radio(
        "Data source",
        ["Use a demo dataset", "Upload my own file"],
        horizontal=True,
        key=f"{key_prefix}_source",
    )
    if source == "Use a demo dataset":
        options = {v["label"]: k for k, v in DEMO_DATASETS.items()}
        label = st.selectbox("Choose a demo dataset", list(options.keys()), key=f"{key_prefix}_demo_select")
        key = options[label]
        info = DEMO_DATASETS[key]
        st.caption(info["description"])
        df = pd.read_csv(info["file"])

        cache_key = f"{key_prefix}_storage_id_{key}"
        if cache_key not in st.session_state:
            file_bytes = open(info["file"], "rb").read()
            st.session_state[cache_key] = storage_manager.save_dataset(
                file_bytes, filename=f"{key}.csv", dataset_name=key, source="demo")
        st.session_state[f"{key_prefix}_storage_id"] = st.session_state[cache_key]
        return key, df
    else:
        uploaded = st.file_uploader(
            "Upload CSV, Excel, JSON, Parquet, TSV, or TXT", type=["csv", "xlsx", "xls", "json", "parquet", "tsv", "txt"],
            key=f"{key_prefix}_uploader",
        )
        if uploaded is not None:
            raw_bytes = uploaded.getvalue()
            name = uploaded.name.rsplit(".", 1)[0]

            cache_key = f"{key_prefix}_storage_id_{uploaded.name}_{len(raw_bytes)}"
            if cache_key not in st.session_state:
                st.session_state[cache_key] = storage_manager.save_dataset(
                    raw_bytes, filename=uploaded.name, dataset_name=name, source="upload")
            st.session_state[f"{key_prefix}_storage_id"] = st.session_state[cache_key]

            df = load_uploaded_file(uploaded)
            return name, df
        return None, None


def score_color(score: float) -> str:
    if score >= 85:
        return "🟢"
    if score >= 65:
        return "🟡"
    return "🔴"
