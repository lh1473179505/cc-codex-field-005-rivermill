"""Event-time window engine: watermark-driven tumbling/sliding/session aggregation."""

from __future__ import annotations

import json
import math
from pathlib import Path

from .checkpoint import empty_checkpoint, load_checkpoint, save_checkpoint
from .errors import FormatError
from .events import Event, iter_jsonl
from .job import Job
from .watermark import WatermarkClock
from .windows import sliding_windows, tumbling_windows


class AggState:
    """Mutable accumulator for every aggregation of one window instance."""

    def __init__(self, job: Job, agg_specs: list):
        self.job = job
        self.agg_specs = agg_specs
        self.count = 0
        self.sums: dict[str, float] = {}
        self.mins: dict[str, float] = {}
        self.maxs: dict[str, float] = {}
        self.distinct: dict[str, set[str]] = {}

    def add(self, event: Event) -> None:
        self.count += 1
        fields = event.fields
        for spec in self.agg_specs:
            if spec.fn == "count":
                continue
            value = fields[spec.field]
            if spec.fn in ("sum", "avg"):
                self.sums[spec.alias] = self.sums.get(spec.alias, 0) + value
            elif spec.fn == "min":
                if spec.alias not in self.mins or value < self.mins[spec.alias]:
                    self.mins[spec.alias] = value
            elif spec.fn == "max":
                if spec.alias not in self.maxs or value > self.maxs[spec.alias]:
                    self.maxs[spec.alias] = value
            elif spec.fn == "distinct_count":
                bucket = self.distinct.setdefault(spec.alias, set())
                bucket.add(str(value))

    def merge(self, other: "AggState") -> None:
        self.count += other.count
        for alias, value in other.sums.items():
            self.sums[alias] = self.sums.get(alias, 0) + value
        for alias, value in other.mins.items():
            if alias not in self.mins or value < self.mins[alias]:
                self.mins[alias] = value
        for alias, value in other.maxs.items():
            if alias not in self.maxs or value > self.maxs[alias]:
                self.maxs[alias] = value
        for alias, values in other.distinct.items():
            self.distinct.setdefault(alias, set()).update(values)

    def values(self) -> dict:
        out: dict = {}
        for spec in self.agg_specs:
            if spec.fn == "count":
                out[spec.alias] = self.count
            elif spec.fn == "sum":
                out[spec.alias] = self.sums.get(spec.alias, 0)
            elif spec.fn == "min":
                out[spec.alias] = self.mins[spec.alias]
            elif spec.fn == "max":
                out[spec.alias] = self.maxs[spec.alias]
            elif spec.fn == "avg":
                out[spec.alias] = self.sums.get(spec.alias, 0) / self.count
            elif spec.fn == "distinct_count":
                out[spec.alias] = len(self.distinct.get(spec.alias, set()))
        return out

    def to_doc(self) -> dict:
        return {
            "count": self.count,
            "sums": self.sums,
            "mins": self.mins,
            "maxs": self.maxs,
            "distinct": {alias: sorted(values) for alias, values in self.distinct.items()},
        }

    @classmethod
    def from_doc(cls, job: Job, agg_specs: list, doc: dict) -> "AggState":
        state = cls(job, agg_specs)
        state.count = int(doc["count"])
        state.sums = dict(doc.get("sums", {}))
        state.mins = dict(doc.get("mins", {}))
        state.maxs = dict(doc.get("maxs", {}))
        state.distinct = {alias: set(values) for alias, values in doc.get("distinct", {}).items()}
        return state


class WinEntry:
    """One window instance: window name + key + [start, end) with its accumulator."""

    def __init__(self, job: Job, spec, key: tuple[str, ...], start: int, end: int):
        self.job = job
        self.spec = spec
        self.key = key
        self.start = start
        self.end = end
        self.agg = AggState(job, spec.aggs)
        self.fired = False
        self.revision = 0
        self.gc = False

    def key_object(self) -> dict:
        return dict(zip(self.job.key_fields, self.key))

    def record(self, is_late: bool, revision: int) -> dict:
        rec = {
            "job": self.job.name,
            "window": self.spec.name,
            "type": self.spec.type,
            "key": self.key_object(),
            "start": self.start,
            "end": self.end,
            "is_late": is_late,
            "revision": revision,
        }
        rec.update(self.agg.values())
        return rec

    def identity(self, is_late: bool, revision: int) -> tuple:
        return (self.spec.name, json.dumps(self.key_object(), sort_keys=True), self.start, self.end, is_late, revision)

    def to_doc(self) -> dict:
        return {
            "key": list(self.key),
            "start": self.start,
            "end": self.end,
            "fired": self.fired,
            "revision": self.revision,
            "gc": self.gc,
            "agg": self.agg.to_doc(),
        }


class Session(WinEntry):
    """A per-key session window whose [start, end) grows as events arrive."""

    def __init__(self, job: Job, spec, key: tuple[str, ...], start: int, end: int):
        super().__init__(job, spec, key, start, end)
        self.min_ts = start
        self.max_ts = end - spec.gap_ms

    def absorb(self, ts: int) -> None:
        if ts < self.min_ts:
            self.min_ts = ts
        if ts > self.max_ts:
            self.max_ts = ts
        self.start = self.min_ts
        self.end = self.max_ts + self.spec.gap_ms


INF = math.inf


def run_job(
    job: Job,
    input_path: str,
    out_dir: str,
    checkpoint_path: str | None = None,
    from_checkpoint: str | None = None,
) -> None:
    ckpt_doc = None
    if from_checkpoint is not None:
        try:
            ckpt_doc = load_checkpoint(from_checkpoint)
        except (OSError, ValueError, KeyError, TypeError) as e:
            raise FormatError(f"cannot load checkpoint {from_checkpoint}: {e}") from e
        if ckpt_doc.get("job") != job.name:
            raise FormatError(
                f"checkpoint {from_checkpoint} belongs to job {ckpt_doc.get('job')!r}, expected {job.name!r}"
            )
    engine = Engine(job, out_dir, ckpt_doc)
    engine.run(input_path, checkpoint_path)


class Engine:
    def __init__(self, job: Job, out_dir: str, ckpt_doc: dict | None):
        self.job = job
        self.out_dir = Path(out_dir)
        self.clock = WatermarkClock(job.watermark.lag_ms)
        self.entries: dict[tuple, WinEntry] = {}
        self.sessions: dict[tuple[str, ...], list[Session]] = {}
        self.emitted: set[tuple] = set()
        self.late_offsets: set[int] = set()
        self.input_offset = 0
        self.first_seen = False
        self._win_f = None
        self._late_f = None
        restore_state = bool(ckpt_doc and ckpt_doc.get("input_offset", 0))
        if ckpt_doc is not None:
            for ident in ckpt_doc.get("emitted", {}).values():
                for parts in ident:
                    self.emitted.add(tuple(parts))
            self.late_offsets = set(ckpt_doc.get("late_offsets", []))
            if restore_state:
                self._restore_state(ckpt_doc)
                self.input_offset = int(ckpt_doc.get("input_offset", 0))

    # ------------------------------------------------------------------ run
    def run(self, input_path: str, checkpoint_path: str | None) -> None:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        resume = self.input_offset > 0 or bool(self.emitted) or bool(self.late_offsets)
        mode = "a" if resume else "w"
        self._win_f = open(self.out_dir / "windows.jsonl", mode, encoding="utf-8")
        self._late_f = open(self.out_dir / "late.jsonl", mode, encoding="utf-8")
        try:
            for event in iter_jsonl(input_path, self.job.event_time_field, self.job.key_fields):
                if event.offset < self.input_offset:
                    continue
                self.input_offset = event.offset + 1
                self._process(event)
            if self.job.watermark.idle_eof:
                self._fire_and_gc(self.clock.advance_to(INF))
            if checkpoint_path is not None:
                self._write_checkpoint(checkpoint_path)
        finally:
            self._win_f.close()
            self._late_f.close()
            self._win_f = None
            self._late_f = None

    # ------------------------------------------------------------- per event
    def _process(self, event: Event) -> None:
        wm_before = self.clock.watermark if self.first_seen else None
        is_late = self.first_seen and event.ts <= wm_before
        self.first_seen = True
        if is_late and wm_before - event.ts > self.job.allowed_lateness_ms:
            if event.offset not in self.late_offsets:
                self.late_offsets.add(event.offset)
                record = {
                    "ts": event.ts,
                    "key": self._key_object(event.key),
                    "reason": "late_exceeded",
                    "event": event.raw,
                }
                self._late_f.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        else:
            self._assign(event, is_late, wm_before)
        watermark = self.clock.observe(event.ts)
        self._fire_and_gc(watermark)

    def _assign(self, event: Event, is_late: bool, wm_before: int | None) -> None:
        for spec in self.job.windows:
            if spec.type == "tumbling":
                ranges = tumbling_windows(event.ts, spec.size_ms, spec.offset_ms)
            elif spec.type == "sliding":
                ranges = sliding_windows(event.ts, spec.size_ms, spec.slide_ms, spec.offset_ms)
            else:
                self._assign_session(spec, event, is_late)
                continue
            for start, end in ranges:
                self._admit(spec, event, event.key, start, end, is_late)

    def _admit(
        self,
        spec,
        event: Event,
        key: tuple[str, ...],
        start: int,
        end: int,
        is_late: bool,
    ) -> None:
        ident = (spec.name, key, start, end)
        entry = self.entries.get(ident)
        if entry is None:
            if is_late:
                # Window was garbage-collected (or never opened): late event cannot enter.
                return
            entry = WinEntry(self.job, spec, key, start, end)
            self.entries[ident] = entry
        elif entry.gc:
            return
        entry.agg.add(event)
        if is_late and entry.fired:
            entry.revision += 1
            self._emit(entry, True, entry.revision)

    # -------------------------------------------------------------- sessions
    def _assign_session(self, spec, event: Event, is_late: bool) -> None:
        key = event.key
        gap = spec.gap_ms
        ts = event.ts
        sessions = self.sessions.setdefault(key, [])
        matches = [s for s in sessions if s.start - gap < ts < s.end + gap]
        if not matches:
            if is_late:
                return
            session = Session(self.job, spec, key, ts, ts + gap)
            session.agg.add(event)
            sessions.append(session)
            sessions.sort(key=lambda s: s.start)
            self.entries[(spec.name, key, session.start, session.end)] = session
            return
        if len(matches) > 1:
            target = self._merge_sessions(spec, key, matches)
        else:
            target = matches[0]
            self.entries.pop((spec.name, key, target.start, target.end), None)
        target.agg.add(event)
        target.absorb(ts)
        self.entries[(spec.name, key, target.start, target.end)] = target
        sessions.sort(key=lambda s: s.start)
        if is_late and target.fired:
            target.revision += 1
            self._emit(target, True, target.revision)

    def _merge_sessions(self, spec, key: tuple[str, ...], matches: list[Session]) -> Session:
        target = matches[0]
        self.entries.pop((spec.name, key, target.start, target.end), None)
        for session in matches[1:]:
            self.entries.pop((spec.name, key, session.start, session.end), None)
            target.agg.merge(session.agg)
            target.fired = target.fired or session.fired
            target.revision = max(target.revision, session.revision)
            target.min_ts = min(target.min_ts, session.min_ts)
            target.max_ts = max(target.max_ts, session.max_ts)
        target.start = target.min_ts
        target.end = target.max_ts + spec.gap_ms
        self.sessions[key] = [s for s in self.sessions[key] if s not in matches[1:]]
        self.entries[(spec.name, key, target.start, target.end)] = target
        return target

    # --------------------------------------------------------- firing and GC
    def _fire_and_gc(self, watermark) -> None:
        due = [e for e in self.entries.values() if not e.fired and e.end <= watermark]
        due.sort(key=lambda e: (e.spec.name, e.key, e.start))
        for entry in due:
            entry.fired = True
            if self.job.triggers.on_watermark:
                self._emit(entry, False, 0)
        for ident, entry in list(self.entries.items()):
            if not entry.gc and watermark >= entry.end + self.job.allowed_lateness_ms:
                entry.gc = True
                if entry.spec.type == "session":
                    bucket = self.sessions.get(entry.key)
                    if bucket and entry in bucket:
                        bucket.remove(entry)

    def _emit(self, entry: WinEntry, is_late: bool, revision: int) -> None:
        identity = entry.identity(is_late, revision)
        if identity in self.emitted:
            return
        self.emitted.add(identity)
        self._win_f.write(
            json.dumps(entry.record(is_late, revision), ensure_ascii=False, separators=(",", ":")) + "\n"
        )

    def _key_object(self, key: tuple[str, ...]) -> dict:
        return dict(zip(self.job.key_fields, key))

    # ------------------------------------------------------------ checkpoint
    def _write_checkpoint(self, path: str) -> None:
        doc = empty_checkpoint(self.job.name)
        doc["input_offset"] = self.input_offset
        doc["watermark"] = None if self.clock.watermark == INF else self.clock.watermark
        doc["max_event_time"] = self.clock.max_event_time
        windows_doc: dict[str, list] = {}
        for (window_name, _key, _start, _end), entry in self.entries.items():
            windows_doc.setdefault(window_name, []).append(entry.to_doc())
        doc["windows"] = windows_doc
        emitted_doc: dict[str, list] = {}
        for identity in self.emitted:
            emitted_doc.setdefault(identity[0], []).append(list(identity))
        doc["emitted"] = emitted_doc
        doc["late_offsets"] = sorted(self.late_offsets)
        save_checkpoint(path, doc)

    def _restore_state(self, doc: dict) -> None:
        watermark = doc.get("watermark")
        if watermark is not None:
            self.clock.advance_to(watermark)
        max_event_time = doc.get("max_event_time")
        if max_event_time is not None and (
            self.clock.max_event_time is None or max_event_time > self.clock.max_event_time
        ):
            self.clock.max_event_time = max_event_time
        self.first_seen = True
        specs = {w.name: w for w in self.job.windows}
        for window_name, items in doc.get("windows", {}).items():
            spec = specs.get(window_name)
            if spec is None:
                continue
            for item in items:
                key = tuple(item["key"])
                if spec.type == "session":
                    entry = Session(self.job, spec, key, item["start"], item["end"])
                    entry.min_ts = item["start"]
                    entry.max_ts = item["end"] - spec.gap_ms
                else:
                    entry = WinEntry(self.job, spec, key, item["start"], item["end"])
                entry.agg = AggState.from_doc(self.job, spec.aggs, item["agg"])
                entry.fired = bool(item["fired"])
                entry.revision = int(item["revision"])
                entry.gc = bool(item.get("gc", False))
                self.entries[(window_name, key, entry.start, entry.end)] = entry
                if spec.type == "session" and not entry.gc:
                    self.sessions.setdefault(key, []).append(entry)
        for bucket in self.sessions.values():
            bucket.sort(key=lambda s: s.start)
