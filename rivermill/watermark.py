"""Watermark tracking for a single source.

The watermark is event-time based: after each observed event it is

    max(previous, max_event_time_seen - lag_ms)

and it never goes backwards. The engine (not implemented here) is also
expected to apply an end-of-file advance when the job sets idle_eof.
"""

from __future__ import annotations


class WatermarkClock:
    def __init__(self, lag_ms: int):
        if lag_ms < 0:
            raise ValueError("lag_ms must be >= 0")
        self.lag_ms = lag_ms
        self.max_event_time: int | None = None
        self.watermark: int = 0  # not yet initialized; observe() returns 0 until first event
        self._initialized = False

    def observe(self, ts: int) -> int:
        if self.max_event_time is None or ts > self.max_event_time:
            self.max_event_time = ts
        candidate = self.max_event_time - self.lag_ms
        if not self._initialized or candidate > self.watermark:
            self.watermark = candidate
            self._initialized = True
        return self.watermark

    def advance_to(self, ts: int) -> int:
        if not self._initialized or ts > self.watermark:
            self.watermark = ts
            self._initialized = True
        return self.watermark
