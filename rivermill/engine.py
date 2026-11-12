"""Event-time window engine. Not implemented in this snapshot."""

from __future__ import annotations

from pathlib import Path

from .errors import NotImplementedEngine
from .job import Job


def run_job(
    job: Job,
    input_path: str,
    out_dir: str,
    checkpoint_path: str | None = None,
    from_checkpoint: str | None = None,
) -> None:
    raise NotImplementedEngine(
        "rivermill.engine.run_job is not implemented in this snapshot"
    )
