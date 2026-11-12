"""Checkpoint document shape. The engine should read and write this JSON."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


CHECKPOINT_VERSION = 1


def empty_checkpoint(job_name: str) -> dict[str, Any]:
    return {
        "version": CHECKPOINT_VERSION,
        "job": job_name,
        "input_offset": 0,  # next JSONL line to read (0-based, including comment lines as counted by the reader)
        "watermark": None,
        "max_event_time": None,
        "windows": {},
        "emitted": {},
    }


def load_checkpoint(path: str) -> dict[str, Any]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if int(data.get("version", 0)) != CHECKPOINT_VERSION:
        raise ValueError(f"unsupported checkpoint version: {data.get('version')}")
    return data


def save_checkpoint(path: str, doc: dict[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(p)
