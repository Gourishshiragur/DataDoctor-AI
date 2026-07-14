"""
Lightweight persistence layer backed by local JSON files.

No external database — deliberate, so the app deploys on free-tier hosting
(Streamlit Community Cloud) with zero provisioning. Streamlit Community Cloud
storage is ephemeral across redeploys, which is an explicit, documented
tradeoff (see README) — acceptable for a showcase/demo app, called out
rather than hidden.
"""
import json
from pathlib import Path
from typing import List

DATA_DIR = Path(__file__).parent.parent / "data"
PIPELINES_FILE = DATA_DIR / "pipelines.json"
RUNS_FILE = DATA_DIR / "runs.json"
LOGS_FILE = DATA_DIR / "logs.jsonl"

DATA_DIR.mkdir(exist_ok=True)


def _load_json(path: Path, default):
    if not path.exists():
        return default
    with open(path) as f:
        return json.load(f)


def _save_json(path: Path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)


def load_pipelines() -> List[dict]:
    return _load_json(PIPELINES_FILE, [])


def save_pipeline(pipeline_dict: dict):
    pipelines = load_pipelines()
    pipelines = [p for p in pipelines if p["id"] != pipeline_dict["id"]]
    pipelines.append(pipeline_dict)
    _save_json(PIPELINES_FILE, pipelines)


def delete_pipeline(pipeline_id: str):
    pipelines = [p for p in load_pipelines() if p["id"] != pipeline_id]
    _save_json(PIPELINES_FILE, pipelines)


def load_runs() -> List[dict]:
    return _load_json(RUNS_FILE, [])


def save_run(run_dict: dict):
    runs = load_runs()
    runs = [r for r in runs if r["id"] != run_dict["id"]]
    runs.append(run_dict)
    _save_json(RUNS_FILE, runs)


def append_log(entry: dict):
    with open(LOGS_FILE, "a") as f:
        f.write(json.dumps(entry, default=str) + "\n")


def load_logs(run_id: str = None) -> List[dict]:
    if not LOGS_FILE.exists():
        return []
    entries = []
    with open(LOGS_FILE) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            entry = json.loads(line)
            if run_id is None or entry.get("run_id") == run_id:
                entries.append(entry)
    return entries
