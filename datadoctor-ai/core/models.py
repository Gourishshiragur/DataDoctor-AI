"""
Data models for DataDoctor AI. Kept as plain dataclasses + dict serialization
(no ORM) since persistence is a flat JSON store — appropriate for a portfolio
app that needs to deploy on a free tier with no database provisioning.
"""
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Optional
import uuid

STEP_TYPES = ["source", "transform", "sink"]
STEP_STATUSES = ["PENDING", "RUNNING", "SUCCEEDED", "FAILED", "SKIPPED"]
RUN_STATUSES = ["RUNNING", "SUCCEEDED", "FAILED", "PARTIALLY_REPAIRED"]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@dataclass
class Step:
    id: str
    name: str
    step_type: str  # source | transform | sink
    engine: str  # pyspark | sql
    code: str
    depends_on: list = field(default_factory=list)


@dataclass
class Pipeline:
    id: str
    name: str
    description: str
    steps: list  # list[Step]
    created_at: str = field(default_factory=_now)

    def to_dict(self):
        d = asdict(self)
        return d


@dataclass
class StepRun:
    step_id: str
    status: str = "PENDING"
    started_at: Optional[str] = None
    ended_at: Optional[str] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    rows_processed: Optional[int] = None


@dataclass
class Run:
    id: str
    pipeline_id: str
    pipeline_name: str
    status: str = "RUNNING"
    started_at: str = field(default_factory=_now)
    ended_at: Optional[str] = None
    step_runs: list = field(default_factory=list)  # list[StepRun]

    def to_dict(self):
        return asdict(self)
