# Event-time semantics

This document is the contract the `run` command must implement. `inspect` already works and does not compute windows.

## Input

Events are JSON objects, one per line, with an integer event-time field (milliseconds) and the job's key fields. Lines that are empty or start with `#` are ignored. Events are processed **in file order** (arrival order), which is not necessarily event-time order.

## Watermarks

For a single file source, keep `max_event_time` seen so far. After each event the watermark is

```
watermark = max(previous_watermark, max_event_time - lag_ms)
```

It never moves backwards. A job with `"idle_eof": true` (the fixtures do) must, after the last event has been applied, advance the watermark to `+∞` so every still-open window that is only waiting for the watermark can close. There is no separate processing-time clock in the fixtures; `early_every_ms` is unused there and may be ignored if it is `null`.

An event is **late** when `ts <= watermark` at the moment it is observed (the watermark used is the one **before** this event updates it, except that the first event starts with no watermark and is never late). Equivalently: after observing earlier events, compare this event's timestamp with the then-current watermark.

## Allowed lateness and drop

- If an event is on time (`ts > watermark`), add it to the relevant windows.
- If it is late and `watermark - ts <= allowed_lateness_ms`, it is still admitted into any window that has not been garbage-collected (a window is GC'd once `watermark >= window_end + allowed_lateness_ms`).
- If it is late by more than `allowed_lateness_ms`, it must **not** change any window aggregate. Write it to the late side output (`late.jsonl`) instead.

## Window assignment

- **Tumbling** `[start, start+size)` with optional `offset_ms`. A helper is already in `rivermill.windows.tumbling_windows`.
- **Sliding** all `[start, start+size)` with `start = offset + k*slide` that contain `ts`. A helper is already in `rivermill.windows.sliding_windows`.
- **Session**, per key: events belong to the same session while each next event's timestamp is **strictly less than** `last_event_time_in_session + gap_ms`. If `ts >= last + gap`, a new session starts. Out-of-order events may join an existing session when `session_start - gap < ts < session_end + gap` (use `session_end` as the maximum event time already in that session) and must merge two sessions when a late/out-of-order event bridges their gap. Session start is the minimum event time in the session; session end used for triggering is `max_ts + gap_ms` (the session stays open until the watermark reaches that end).

## Aggregations

Per window instance (job window name + key + `[start,end)`):

| fn | meaning |
|---|---|
| count | number of admitted events |
| sum | sum of a numeric field |
| min / max | min/max of a numeric field |
| avg | sum/count of a numeric field (null if count is 0, which should not be emitted) |
| distinct_count | number of distinct stringified values of a field |

## Triggers and append output

`output_mode` is `append`:

- When the watermark first reaches or passes `window_end`, emit one **on-time** record for that window (`is_late=false`, `revision=0`) if `triggers.on_watermark` is true.
- If a late-but-admitted event later changes that window, emit an additional record with `is_late=true` and `revision` incremented (1, 2, …). Do not rewrite or delete the on-time record.
- Do not emit a window that received no events.

Each output record is one JSON object written to `windows.jsonl` in the output directory, with at least:

```
{
  "job": "...",
  "window": "<window name>",
  "type": "tumbling" | "sliding" | "session",
  "key": {"shop_id": "A"},
  "start": <ms>,
  "end": <ms>,
  "is_late": false,
  "revision": 0,
  "n": ...,
  "sum_amount": ...,
  "...other agg aliases...": ...
}
```

`key` is an object using the job's `key_fields`. Aggregation aliases come from the job (`as`). Records should be written in watermark-fire order (and late refinements at the moment they happen). Ties can be broken by window name, then key, then start.

Late side-output records in `late.jsonl`:

```
{"ts": ..., "key": {"shop_id": "A"}, "reason": "late_exceeded", "event": {<original event>}}
```

## Checkpoints

If `--checkpoint` is given, write a JSON checkpoint after finishing the input (and also usable mid-run if you want). The helper in `rivermill.checkpoint` shows the expected version and top-level keys. `--from-checkpoint` must resume so that replaying the remainder of the same file, or replaying the whole file with the checkpoint's emitted set, does **not** duplicate already-emitted `(window, key, start, end, is_late, revision)` records. The concatenation of outputs from a split run must match a single clean run on the same input (same records, not necessarily identical byte layout if you sort equivalently — record *content* must match).

## Compatibility

Do not change the job JSON schema or the fixture events. `python -m rivermill inspect` must keep working.
