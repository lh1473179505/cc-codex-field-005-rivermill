"""Pure helpers for assigning event times to tumbling / sliding windows.

Session assignment depends on per-key history and lives in the engine
(not implemented in this snapshot).
"""

from __future__ import annotations


def tumbling_start(ts: int, size_ms: int, offset_ms: int = 0) -> int:
    """Inclusive start of the unique tumbling window that contains ts."""
    if size_ms <= 0:
        raise ValueError("size_ms must be > 0")
    # Floor in a way that works for ts >= offset. Fixtures use non-negative ts.
    rel = ts - offset_ms
    start = (rel // size_ms) * size_ms + offset_ms
    return int(start)


def tumbling_windows(ts: int, size_ms: int, offset_ms: int = 0) -> list[tuple[int, int]]:
    start = tumbling_start(ts, size_ms, offset_ms)
    return [(start, start + size_ms)]


def sliding_windows(ts: int, size_ms: int, slide_ms: int, offset_ms: int = 0) -> list[tuple[int, int]]:
    """All sliding windows [start, start+size) that contain ts.

    start = offset + k*slide, for the integers k such that start <= ts < start+size.
    """
    if size_ms <= 0 or slide_ms <= 0:
        raise ValueError("size_ms and slide_ms must be > 0")
    # The latest start that can contain ts is the greatest start <= ts:
    rel = ts - offset_ms
    last_start = (rel // slide_ms) * slide_ms + offset_ms
    out: list[tuple[int, int]] = []
    start = last_start
    while start > ts - size_ms:
        # start <= ts is implied by construction for the first value; we walk backward
        if start <= ts < start + size_ms:
            out.append((start, start + size_ms))
        start -= slide_ms
        if start + size_ms <= ts and start < last_start - size_ms - slide_ms:
            break
    # include the first if the loop skipped equal case... last_start always valid
    if not out or out[0] != (last_start, last_start + size_ms):
        if last_start <= ts < last_start + size_ms:
            out.insert(0, (last_start, last_start + size_ms))
    # unique, descending starts then sort ascending
    uniq = sorted(set(out))
    return uniq
