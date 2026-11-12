# rivermill

A small event-time window aggregator for JSONL streams. This snapshot already has:

- Job JSON loading (`jobs/shop_windows.json`)
- A JSONL event reader
- Tumbling / sliding window assignment helpers
- A watermark clock helper
- JSONL sink and checkpoint document helpers
- `python -m rivermill inspect`

The engine in `rivermill/engine.py` implements `python -m rivermill run` following
`docs/semantics.md`: watermark-driven tumbling/sliding/session windows, allowed-lateness
handling with a `late.jsonl` side output, append-mode revisions, and resumable checkpoints.

## Requirements

- Python 3.11+ (stdlib only)
- No containers. Do not add Docker files. Do not add Flink, Spark, Kafka, or other cluster dependencies.

## Usage

```bash
python -m rivermill inspect --job jobs/shop_windows.json --input fixtures/events.jsonl
python -m rivermill run --job jobs/shop_windows.json --input fixtures/events.jsonl --out /tmp/rivermill-out --checkpoint /tmp/rivermill-out/ckpt.json
# resume: already-emitted records are deduplicated, open window state is restored
python -m rivermill run --job jobs/shop_windows.json --input fixtures/events.jsonl --out /tmp/rivermill-out --from-checkpoint /tmp/rivermill-out/ckpt.json
```

A run writes `windows.jsonl` (on-time records with `is_late=false, revision=0`, plus
append-only late refinements) and `late.jsonl` (events later than `allowed_lateness_ms`).

## Layout

- `rivermill/` — library and CLI
- `docs/semantics.md` — watermark, lateness, session, trigger, and output contract
- `jobs/` — job definitions
- `fixtures/events.jsonl` — out-of-order and late events for two keys
