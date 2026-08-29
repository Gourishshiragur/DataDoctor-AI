"""
storage/manager.py
--------------------
Storage Manager (Phase 1 of the roadmap): the layer between "a file the user gave us"
and everything downstream. Nothing goes straight from an upload widget into a
Databricks Volume — it lands here first:

    Upload -> Storage Manager (local file + SQLite registry) -> Databricks (Demo/Enterprise)

Why this matters in practice:
- Rerun without re-upload: PipelineStudio's "Run as native Databricks Job" button reads
  the persisted bytes via `load_dataset_bytes(dataset_id)`, not a re-serialized
  in-memory DataFrame — so re-running a job after a Streamlit rerun (which clears
  widget state) doesn't require the user to upload the file again.
- Dataset history: every upload (and every demo-dataset selection) gets a row in the
  `datasets` registry table (database/history.py), independent of any particular
  pipeline run — you can see what's been tried without cross-referencing run logs.
- Resilience: if Databricks is unreachable, the file is already safely on local disk;
  nothing is lost or has to be re-requested from the browser.

Storage location: DATA_DIR (local disk under the app root) for this deployment. For a
real multi-instance/production deployment this would move to a shared location
(Unity Catalog Volume, ADLS, S3) — kept local-disk here to match the "local file
storage" instruction and because it works with zero cloud setup for Demo Mode.
"""
from __future__ import annotations

import hashlib
import io
import uuid
from pathlib import Path

import pandas as pd

from database import history

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data" / "uploads"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def _checksum(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()[:16]


def save_dataset(file_bytes: bytes, filename: str, dataset_name: str, source: str = "upload") -> str:
    """Persists raw file bytes to local storage and registers the dataset. Returns the
    dataset_id to use for every downstream operation (job submission, rerun, etc.)."""
    dataset_id = str(uuid.uuid4())[:12]
    dataset_dir = DATA_DIR / dataset_id
    dataset_dir.mkdir(parents=True, exist_ok=True)
    file_path = dataset_dir / filename
    file_path.write_bytes(file_bytes)

    try:
        row_count = len(_bytes_to_df(file_bytes, filename))
    except Exception:  # noqa: BLE001 — row count is nice-to-have, never blocks the save
        row_count = -1

    history.register_dataset(
        dataset_id=dataset_id, name=dataset_name, source=source, original_filename=filename,
        file_path=str(file_path), row_count=row_count, byte_size=len(file_bytes),
        checksum=_checksum(file_bytes),
    )
    history.log_audit("local-user", "dataset_registered", dataset_id,
                       {"name": dataset_name, "source": source, "byte_size": len(file_bytes)})
    return dataset_id


def _bytes_to_df(file_bytes: bytes, filename: str) -> pd.DataFrame:
    buf = io.BytesIO(file_bytes)
    name = filename.lower()
    if name.endswith(".csv"):
        return pd.read_csv(buf)
    if name.endswith((".xlsx", ".xls")):
        return pd.read_excel(buf)
    if name.endswith(".json"):
        return pd.read_json(buf)
    if name.endswith(".parquet"):
        return pd.read_parquet(buf)
    raise ValueError(f"Unsupported file type: {filename}")


def load_dataset_bytes(dataset_id: str) -> bytes:
    record = history.get_dataset(dataset_id)
    if not record:
        raise FileNotFoundError(f"No registered dataset with id {dataset_id}")
    return Path(record["file_path"]).read_bytes()


def load_dataset_df(dataset_id: str) -> pd.DataFrame:
    record = history.get_dataset(dataset_id)
    if not record:
        raise FileNotFoundError(f"No registered dataset with id {dataset_id}")
    return _bytes_to_df(load_dataset_bytes(dataset_id), record["original_filename"])


def list_datasets(limit: int = 50) -> list[dict]:
    return history.list_datasets(limit)


def get_dataset(dataset_id: str) -> dict | None:
    return history.get_dataset(dataset_id)
