"""
Data models for DataDoctor AI.

Kept as plain dataclasses with dictionary serialization because persistence
uses a flat JSON store. This approach is suitable for a portfolio application
that needs to deploy on a free tier without database provisioning.
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Optional
import uuid


# ============================================================
# Supported model values
# ============================================================

STEP_TYPES = [
    "source",
    "transform",
    "sink",
]

STEP_STATUSES = [
    "PENDING",
    "RUNNING",
    "SUCCEEDED",
    "FAILED",
    "SKIPPED",
]

RUN_STATUSES = [
    "RUNNING",
    "SUCCEEDED",
    "FAILED",
    "PARTIALLY_REPAIRED",
    "REPAIRED",
]


# ============================================================
# Utility functions
# ============================================================

def _now() -> str:
    """
    Return the current timezone-aware UTC timestamp.
    """

    return datetime.now(
        timezone.utc
    ).isoformat()


def new_id(
    prefix: str,
) -> str:
    """
    Generate a short unique identifier with a readable prefix.

    Examples:
        pipeline-a1b2c3d4
        run-e5f6a7b8
        step-1234abcd
    """

    return (
        f"{prefix}-"
        f"{uuid.uuid4().hex[:8]}"
    )


# ============================================================
# Pipeline models
# ============================================================

@dataclass
class Step:
    """
    Represents one executable pipeline step.

    Supported step types:
    - source
    - transform
    - sink

    Dependencies are stored using pipeline step IDs.
    """

    id: str
    name: str
    step_type: str
    engine: str
    code: str
    depends_on: list = field(
        default_factory=list
    )

    def to_dict(
        self,
    ) -> dict:
        """
        Convert the step into a JSON-serializable dictionary.
        """

        return asdict(
            self
        )


@dataclass
class Pipeline:
    """
    Represents a complete DataDoctor AI data pipeline.

    The steps collection contains Step objects connected using
    dependency IDs.
    """

    id: str
    name: str
    description: str
    steps: list
    created_at: str = field(
        default_factory=_now
    )

    def to_dict(
        self,
    ) -> dict:
        """
        Convert the complete pipeline into a dictionary.
        """

        return asdict(
            self
        )


# ============================================================
# Pipeline execution models
# ============================================================

@dataclass
class StepRun:
    """
    Stores execution information for one pipeline step.

    Includes:
    - execution status
    - execution timestamps
    - error information
    - retry count
    - processed-row count
    """

    step_id: str

    status: str = (
        "PENDING"
    )

    started_at: Optional[
        str
    ] = None

    ended_at: Optional[
        str
    ] = None

    error_message: Optional[
        str
    ] = None

    retry_count: int = 0

    rows_processed: Optional[
        int
    ] = None

    def to_dict(
        self,
    ) -> dict:
        """
        Convert the step execution state into a dictionary.
        """

        return asdict(
            self
        )


@dataclass
class Run:
    """
    Represents one complete pipeline execution.

    A recovered run uses REPAIRED rather than SUCCEEDED so
    normal first-attempt success remains distinguishable from
    success after retry or automated repair.
    """

    id: str

    pipeline_id: str

    pipeline_name: str

    status: str = (
        "RUNNING"
    )

    started_at: str = field(
        default_factory=_now
    )

    ended_at: Optional[
        str
    ] = None

    step_runs: list = field(
        default_factory=list
    )

    def to_dict(
        self,
    ) -> dict:
        """
        Convert the complete pipeline run into a dictionary.
        """

        return asdict(
            self
        )