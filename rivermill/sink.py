"""Append-only JSONL sink helpers (usable once the engine writes records)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable


def write_jsonl(path: str, records: Iterable[dict[str, Any]]) -> int:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with open(p, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")
            n += 1
    return n
