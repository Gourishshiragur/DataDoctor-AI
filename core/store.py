"""
DataDoctor AI — Enterprise Local Persistence

Lightweight persistence backed by local JSON and JSONL files.

Design goals:
- No external database required
- Free-tier deployment friendly
- Preserve existing pipeline, run, and log APIs
- Safe JSON reads and atomic writes
- Analysis history
- Uploaded-file metadata
- Business insight history
- Audit trail

Important deployment trade-off:
Streamlit Community Cloud storage may be ephemeral across
redeployments. This is suitable for a showcase/demo deployment.
Production deployments should replace this layer with durable
object storage or a managed database.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


# ============================================================
# Storage paths
# ============================================================

DATA_DIR = (
    Path(__file__).resolve().parent.parent
    / "data"
)

DATA_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

PIPELINES_FILE = (
    DATA_DIR
    / "pipelines.json"
)

RUNS_FILE = (
    DATA_DIR
    / "runs.json"
)

LOGS_FILE = (
    DATA_DIR
    / "logs.jsonl"
)

ANALYSES_FILE = (
    DATA_DIR
    / "analyses.json"
)

UPLOADS_FILE = (
    DATA_DIR
    / "uploads.json"
)

BUSINESS_INSIGHTS_FILE = (
    DATA_DIR
    / "business_insights.json"
)

AUDIT_FILE = (
    DATA_DIR
    / "audit.jsonl"
)


# ============================================================
# General utilities
# ============================================================

def _utc_now() -> str:
    """
    Return a timezone-aware UTC timestamp.
    """

    return (
        datetime.now(
            timezone.utc
        )
        .isoformat()
    )


def _load_json(
    path: Path,
    default: Any,
) -> Any:
    """
    Safely load JSON.

    Missing, empty, or corrupted files return the supplied
    default rather than crashing the Streamlit application.
    """

    if not path.exists():
        return default

    try:
        content = path.read_text(
            encoding="utf-8"
        )

        if not content.strip():
            return default

        return json.loads(
            content
        )

    except (
        json.JSONDecodeError,
        OSError,
        UnicodeDecodeError,
    ):
        return default


def _save_json(
    path: Path,
    data: Any,
) -> None:
    """
    Atomically save JSON.

    Data is written to a temporary file first and then moved
    into place. This reduces the risk of leaving a partially
    written JSON file if the application stops during a write.
    """

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    file_descriptor = None
    temporary_path = None

    try:
        (
            file_descriptor,
            temporary_name,
        ) = tempfile.mkstemp(
            prefix=(
                f"{path.stem}_"
            ),
            suffix=".tmp",
            dir=str(
                path.parent
            ),
        )

        temporary_path = Path(
            temporary_name
        )

        with os.fdopen(
            file_descriptor,
            "w",
            encoding="utf-8",
        ) as file:
            file_descriptor = None

            json.dump(
                data,
                file,
                indent=2,
                ensure_ascii=False,
                default=str,
            )

            file.flush()

            try:
                os.fsync(
                    file.fileno()
                )

            except OSError:
                pass

        temporary_path.replace(
            path
        )

    finally:
        if (
            file_descriptor
            is not None
        ):
            try:
                os.close(
                    file_descriptor
                )

            except OSError:
                pass

        if (
            temporary_path
            is not None
            and temporary_path.exists()
        ):
            try:
                temporary_path.unlink()

            except OSError:
                pass


def _append_json_line(
    path: Path,
    entry: Dict[str, Any],
) -> None:
    """
    Append one UTF-8 JSON record to a JSONL file.
    """

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with path.open(
        "a",
        encoding="utf-8",
    ) as file:
        file.write(
            json.dumps(
                entry,
                ensure_ascii=False,
                default=str,
            )
            + "\n"
        )


def _load_json_lines(
    path: Path,
) -> List[dict]:
    """
    Safely load valid JSONL records.

    One malformed log line does not prevent valid records from
    loading.
    """

    if not path.exists():
        return []

    entries: List[dict] = []

    try:
        with path.open(
            "r",
            encoding="utf-8",
        ) as file:
            for line in file:
                line = line.strip()

                if not line:
                    continue

                try:
                    entry = json.loads(
                        line
                    )

                except (
                    json.JSONDecodeError,
                    TypeError,
                ):
                    continue

                if isinstance(
                    entry,
                    dict,
                ):
                    entries.append(
                        entry
                    )

    except OSError:
        return []

    return entries


def _upsert_record(
    path: Path,
    record: Dict[str, Any],
    identifier_key: str = "id",
) -> None:
    """
    Insert or update a dictionary in a JSON list.
    """

    records = _load_json(
        path,
        [],
    )

    if not isinstance(
        records,
        list,
    ):
        records = []

    identifier = record.get(
        identifier_key
    )

    if identifier is None:
        records.append(
            record
        )

    else:
        records = [
            existing
            for existing
            in records
            if not (
                isinstance(
                    existing,
                    dict,
                )
                and existing.get(
                    identifier_key
                )
                == identifier
            )
        ]

        records.append(
            record
        )

    _save_json(
        path,
        records,
    )


def _delete_record(
    path: Path,
    identifier: str,
    identifier_key: str = "id",
) -> None:
    """
    Delete a record from a JSON list.
    """

    records = _load_json(
        path,
        [],
    )

    if not isinstance(
        records,
        list,
    ):
        records = []

    records = [
        record
        for record
        in records
        if not (
            isinstance(
                record,
                dict,
            )
            and record.get(
                identifier_key
            )
            == identifier
        )
    ]

    _save_json(
        path,
        records,
    )


def _new_id(
    prefix: str,
) -> str:
    """
    Create a short readable identifier.
    """

    import uuid

    return (
        f"{prefix}_"
        f"{uuid.uuid4().hex[:12]}"
    )


# ============================================================
# Existing pipeline API
# ============================================================

def load_pipelines() -> List[dict]:
    """
    Load configured pipelines.

    Existing application imports remain unchanged.
    """

    pipelines = _load_json(
        PIPELINES_FILE,
        [],
    )

    return (
        pipelines
        if isinstance(
            pipelines,
            list,
        )
        else []
    )


def save_pipeline(
    pipeline_dict: dict,
) -> None:
    """
    Insert or update a pipeline.
    """

    if not isinstance(
        pipeline_dict,
        dict,
    ):
        raise TypeError(
            "pipeline_dict must be a dictionary."
        )

    if not pipeline_dict.get(
        "id"
    ):
        raise ValueError(
            "Pipeline requires an 'id'."
        )

    record = dict(
        pipeline_dict
    )

    record.setdefault(
        "updated_at",
        _utc_now(),
    )

    _upsert_record(
        PIPELINES_FILE,
        record,
    )


def delete_pipeline(
    pipeline_id: str,
) -> None:
    """
    Delete a pipeline by ID.
    """

    _delete_record(
        PIPELINES_FILE,
        pipeline_id,
    )


# ============================================================
# Existing run API
# ============================================================

def load_runs() -> List[dict]:
    """
    Load all pipeline runs.
    """

    runs = _load_json(
        RUNS_FILE,
        [],
    )

    return (
        runs
        if isinstance(
            runs,
            list,
        )
        else []
    )


def save_run(
    run_dict: dict,
) -> None:
    """
    Insert or update a pipeline run.
    """

    if not isinstance(
        run_dict,
        dict,
    ):
        raise TypeError(
            "run_dict must be a dictionary."
        )

    if not run_dict.get(
        "id"
    ):
        raise ValueError(
            "Run requires an 'id'."
        )

    record = dict(
        run_dict
    )

    record.setdefault(
        "updated_at",
        _utc_now(),
    )

    _upsert_record(
        RUNS_FILE,
        record,
    )


def delete_run(
    run_id: str,
) -> None:
    """
    Delete a stored run.
    """

    _delete_record(
        RUNS_FILE,
        run_id,
    )


# ============================================================
# Existing application logs API
# ============================================================

def append_log(
    entry: dict,
) -> None:
    """
    Append an application or pipeline log.

    Existing callers remain compatible.
    """

    if not isinstance(
        entry,
        dict,
    ):
        raise TypeError(
            "Log entry must be a dictionary."
        )

    log_entry = dict(
        entry
    )

    log_entry.setdefault(
        "timestamp",
        _utc_now(),
    )

    _append_json_line(
        LOGS_FILE,
        log_entry,
    )


def load_logs(
    run_id: Optional[str] = None,
) -> List[dict]:
    """
    Load application logs.

    When run_id is supplied, only records belonging to that
    run are returned.
    """

    entries = (
        _load_json_lines(
            LOGS_FILE
        )
    )

    if run_id is None:
        return entries

    return [
        entry
        for entry
        in entries
        if entry.get(
            "run_id"
        )
        == run_id
    ]


# ============================================================
# Enterprise analysis history
# ============================================================

def load_analyses() -> List[dict]:
    """
    Load saved dataset-analysis results.
    """

    analyses = _load_json(
        ANALYSES_FILE,
        [],
    )

    return (
        analyses
        if isinstance(
            analyses,
            list,
        )
        else []
    )


def save_analysis(
    analysis_dict: dict,
) -> dict:
    """
    Save or update enterprise analysis results.

    Supported content may include:
    - dataset profile
    - schema
    - quality score
    - missing-value findings
    - duplicate findings
    - anomaly findings
    - business outcomes
    - recommended actions
    """

    if not isinstance(
        analysis_dict,
        dict,
    ):
        raise TypeError(
            "analysis_dict must be a dictionary."
        )

    record = dict(
        analysis_dict
    )

    record.setdefault(
        "id",
        _new_id(
            "analysis"
        ),
    )

    record.setdefault(
        "created_at",
        _utc_now(),
    )

    record["updated_at"] = (
        _utc_now()
    )

    _upsert_record(
        ANALYSES_FILE,
        record,
    )

    return record


def get_analysis(
    analysis_id: str,
) -> Optional[dict]:
    """
    Retrieve one analysis by ID.
    """

    for analysis in (
        load_analyses()
    ):
        if analysis.get(
            "id"
        ) == analysis_id:
            return analysis

    return None


def delete_analysis(
    analysis_id: str,
) -> None:
    """
    Delete an analysis record.
    """

    _delete_record(
        ANALYSES_FILE,
        analysis_id,
    )


# ============================================================
# Uploaded-file metadata
# ============================================================

def load_uploads() -> List[dict]:
    """
    Load uploaded-file metadata.

    Raw uploaded file content is intentionally not stored in
    this JSON file.
    """

    uploads = _load_json(
        UPLOADS_FILE,
        [],
    )

    return (
        uploads
        if isinstance(
            uploads,
            list,
        )
        else []
    )


def save_upload(
    upload_dict: dict,
) -> dict:
    """
    Save uploaded-file metadata.

    Example fields:
    - id
    - file_name
    - file_type
    - file_size
    - rows
    - columns
    - checksum
    - status
    """

    if not isinstance(
        upload_dict,
        dict,
    ):
        raise TypeError(
            "upload_dict must be a dictionary."
        )

    record = dict(
        upload_dict
    )

    record.setdefault(
        "id",
        _new_id(
            "upload"
        ),
    )

    record.setdefault(
        "uploaded_at",
        _utc_now(),
    )

    record["updated_at"] = (
        _utc_now()
    )

    _upsert_record(
        UPLOADS_FILE,
        record,
    )

    return record


def delete_upload(
    upload_id: str,
) -> None:
    """
    Delete uploaded-file metadata.
    """

    _delete_record(
        UPLOADS_FILE,
        upload_id,
    )


# ============================================================
# Business outcomes and recommendations
# ============================================================

def load_business_insights() -> List[dict]:
    """
    Load saved business insights.
    """

    insights = _load_json(
        BUSINESS_INSIGHTS_FILE,
        [],
    )

    return (
        insights
        if isinstance(
            insights,
            list,
        )
        else []
    )


def save_business_insight(
    insight_dict: dict,
) -> dict:
    """
    Save a business finding or recommended action.

    Example fields:
    - finding
    - evidence
    - impact
    - recommendation
    - priority
    - confidence
    - analysis_id
    """

    if not isinstance(
        insight_dict,
        dict,
    ):
        raise TypeError(
            "insight_dict must be a dictionary."
        )

    record = dict(
        insight_dict
    )

    record.setdefault(
        "id",
        _new_id(
            "insight"
        ),
    )

    record.setdefault(
        "created_at",
        _utc_now(),
    )

    record["updated_at"] = (
        _utc_now()
    )

    _upsert_record(
        BUSINESS_INSIGHTS_FILE,
        record,
    )

    return record


def delete_business_insight(
    insight_id: str,
) -> None:
    """
    Delete one saved business insight.
    """

    _delete_record(
        BUSINESS_INSIGHTS_FILE,
        insight_id,
    )


# ============================================================
# Enterprise audit trail
# ============================================================

def append_audit_event(
    event_type: str,
    action: str,
    status: str = "success",
    details: Optional[
        Dict[str, Any]
    ] = None,
    actor: str = "system",
    resource_id: Optional[
        str
    ] = None,
) -> dict:
    """
    Add a structured audit event.

    This supports traceability without requiring an external
    logging platform.
    """

    event = {
        "id": _new_id(
            "audit"
        ),
        "timestamp": (
            _utc_now()
        ),
        "event_type": (
            str(
                event_type
            )
        ),
        "action": (
            str(
                action
            )
        ),
        "status": (
            str(
                status
            )
        ),
        "actor": (
            str(
                actor
            )
        ),
        "resource_id": (
            resource_id
        ),
        "details": (
            details
            or {}
        ),
    }

    _append_json_line(
        AUDIT_FILE,
        event,
    )

    return event


def load_audit_events(
    event_type: Optional[
        str
    ] = None,
    status: Optional[
        str
    ] = None,
    limit: Optional[
        int
    ] = None,
) -> List[dict]:
    """
    Load audit events with optional filters.
    """

    events = (
        _load_json_lines(
            AUDIT_FILE
        )
    )

    if event_type:
        events = [
            event
            for event
            in events
            if event.get(
                "event_type"
            )
            == event_type
        ]

    if status:
        events = [
            event
            for event
            in events
            if event.get(
                "status"
            )
            == status
        ]

    events.sort(
        key=lambda event: (
            event.get(
                "timestamp",
                "",
            )
        ),
        reverse=True,
    )

    if limit is not None:
        events = events[
            :max(
                0,
                int(
                    limit
                ),
            )
        ]

    return events


# ============================================================
# Storage health and summary
# ============================================================

def storage_summary() -> Dict[str, Any]:
    """
    Return storage health information for monitoring pages.
    """

    return {
        "backend": (
            "Local JSON/JSONL"
        ),
        "persistent_database": (
            False
        ),
        "data_directory": (
            str(
                DATA_DIR
            )
        ),
        "pipeline_count": (
            len(
                load_pipelines()
            )
        ),
        "run_count": (
            len(
                load_runs()
            )
        ),
        "log_count": (
            len(
                load_logs()
            )
        ),
        "analysis_count": (
            len(
                load_analyses()
            )
        ),
        "upload_count": (
            len(
                load_uploads()
            )
        ),
        "business_insight_count": (
            len(
                load_business_insights()
            )
        ),
        "audit_event_count": (
            len(
                load_audit_events()
            )
        ),
        "free_tier_compatible": (
            True
        ),
        "production_note": (
            "Use durable object storage or a managed "
            "database for production persistence."
        ),
    }