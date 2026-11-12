"""JSONL event reader."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterator

from .errors import FormatError


@dataclass(frozen=True)
class Event:
    ts: int  # event time, epoch milliseconds
    key: tuple[str, ...]
    fields: dict[str, Any]
    offset: int  # 0-based line number in the source file
    raw: dict[str, Any]


def iter_jsonl(path: str, event_time_field: str, key_fields: list[str]) -> Iterator[Event]:
    with open(path, "r", encoding="utf-8") as f:
        for offset, line in enumerate(f):
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            try:
                raw = json.loads(line)
            except json.JSONDecodeError as e:
                raise FormatError(f"{path}:{offset+1}: invalid JSON: {e}") from e
            if not isinstance(raw, dict):
                raise FormatError(f"{path}:{offset+1}: event must be a JSON object")
            if event_time_field not in raw:
                raise FormatError(f"{path}:{offset+1}: missing event time field {event_time_field!r}")
            try:
                ts = int(raw[event_time_field])
            except (TypeError, ValueError) as e:
                raise FormatError(f"{path}:{offset+1}: bad event time: {e}") from e
            key_parts: list[str] = []
            for k in key_fields:
                if k not in raw:
                    raise FormatError(f"{path}:{offset+1}: missing key field {k!r}")
                key_parts.append(str(raw[k]))
            yield Event(ts=ts, key=tuple(key_parts), fields=raw, offset=offset, raw=raw)
