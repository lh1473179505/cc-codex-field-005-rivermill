"""Load a rivermill job JSON."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from .errors import FormatError


@dataclass(frozen=True)
class AggSpec:
    fn: str
    field: str | None
    alias: str


@dataclass(frozen=True)
class WindowSpec:
    type: str  # tumbling | sliding | session
    size_ms: int | None
    slide_ms: int | None
    offset_ms: int
    gap_ms: int | None
    aggs: list[AggSpec]
    name: str


@dataclass(frozen=True)
class WatermarkSpec:
    lag_ms: int
    idle_eof: bool  # if true, end-of-file advances watermark to +inf so remaining windows close


@dataclass(frozen=True)
class TriggerSpec:
    on_watermark: bool
    early_every_ms: int | None
    on_late: bool


@dataclass(frozen=True)
class Job:
    name: str
    event_time_field: str
    key_fields: list[str]
    watermark: WatermarkSpec
    allowed_lateness_ms: int
    windows: list[WindowSpec]
    triggers: TriggerSpec
    output_mode: str  # append
    source_path: str = ""


ALLOWED_AGGS = {"count", "sum", "min", "max", "avg", "distinct_count"}
ALLOWED_WINDOWS = {"tumbling", "sliding", "session"}


def load_job(path: str) -> Job:
    p = Path(path)
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise FormatError(f"invalid job JSON in {path}: {e}") from e
    if not isinstance(data, dict):
        raise FormatError("job must be a JSON object")
    name = str(data.get("name") or p.stem)
    et = str(data.get("event_time_field", "ts"))
    keys = data.get("key_fields")
    if not isinstance(keys, list) or not keys or not all(isinstance(k, str) for k in keys):
        raise FormatError("key_fields must be a non-empty list of strings")
    wm = data.get("watermarks") or {}
    watermark = WatermarkSpec(
        lag_ms=int(wm.get("lag_ms", 0)),
        idle_eof=bool(wm.get("idle_eof", True)),
    )
    if watermark.lag_ms < 0:
        raise FormatError("lag_ms must be >= 0")
    allowed = int(data.get("allowed_lateness_ms", 0))
    if allowed < 0:
        raise FormatError("allowed_lateness_ms must be >= 0")
    windows: list[WindowSpec] = []
    for i, w in enumerate(data.get("windows") or []):
        typ = w.get("type")
        if typ not in ALLOWED_WINDOWS:
            raise FormatError(f"windows[{i}].type must be one of {sorted(ALLOWED_WINDOWS)}")
        aggs: list[AggSpec] = []
        for j, a in enumerate(w.get("aggs") or []):
            fn = a.get("fn")
            if fn not in ALLOWED_AGGS:
                raise FormatError(f"windows[{i}].aggs[{j}].fn must be one of {sorted(ALLOWED_AGGS)}")
            field = a.get("field")
            if fn != "count" and not field:
                raise FormatError(f"windows[{i}].aggs[{j}] needs a field")
            alias = str(a.get("as") or (fn if fn == "count" else f"{fn}_{field}"))
            aggs.append(AggSpec(fn=fn, field=None if fn == "count" else str(field), alias=alias))
        if not aggs:
            raise FormatError(f"windows[{i}] needs at least one agg")
        wname = str(w.get("name") or f"{typ}_{i}")
        windows.append(
            WindowSpec(
                type=typ,
                size_ms=int(w["size_ms"]) if typ in ("tumbling", "sliding") else None,
                slide_ms=int(w["slide_ms"]) if typ == "sliding" else (int(w["size_ms"]) if typ == "tumbling" else None),
                offset_ms=int(w.get("offset_ms", 0)),
                gap_ms=int(w["gap_ms"]) if typ == "session" else None,
                aggs=aggs,
                name=wname,
            )
        )
        if typ == "tumbling" and windows[-1].size_ms <= 0:
            raise FormatError(f"windows[{i}].size_ms must be > 0")
        if typ == "sliding" and (windows[-1].size_ms <= 0 or windows[-1].slide_ms <= 0):
            raise FormatError(f"windows[{i}] size_ms/slide_ms must be > 0")
        if typ == "session" and windows[-1].gap_ms <= 0:
            raise FormatError(f"windows[{i}].gap_ms must be > 0")
    if not windows:
        raise FormatError("job needs at least one window")
    tr = data.get("triggers") or {}
    early = tr.get("early_every_ms")
    triggers = TriggerSpec(
        on_watermark=bool(tr.get("on_watermark", True)),
        early_every_ms=int(early) if early is not None else None,
        on_late=bool(tr.get("on_late", True)),
    )
    mode = str(data.get("output_mode", "append"))
    if mode != "append":
        raise FormatError("only output_mode=append is defined in this snapshot")
    return Job(
        name=name,
        event_time_field=et,
        key_fields=list(keys),
        watermark=watermark,
        allowed_lateness_ms=allowed,
        windows=windows,
        triggers=triggers,
        output_mode=mode,
        source_path=str(p),
    )
