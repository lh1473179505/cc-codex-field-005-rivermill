# rivermill

A small event-time window aggregator for JSONL streams. This snapshot already has:

- Job JSON loading (`jobs/shop_windows.json`)
- A JSONL event reader
- Tumbling / sliding window assignment helpers
- A watermark clock helper
- JSONL sink and checkpoint document helpers
- `python -m rivermill inspect`

The engine itself is **not** implemented. `python -m rivermill run` is the stub you are expected to fill in, following `docs/semantics.md`.

## Requirements

- Python 3.11+ (stdlib only)
- No containers. Do not add Docker files. Do not add Flink, Spark, Kafka, or other cluster dependencies.

## Usage

```bash
python -m rivermill inspect --job jobs/shop_windows.json --input fixtures/events.jsonl
python -m rivermill run --job jobs/shop_windows.json --input fixtures/events.jsonl --out /tmp/rivermill-out --checkpoint /tmp/rivermill-out/ckpt.json
```

## Layout

- `rivermill/` — library and CLI
- `docs/semantics.md` — watermark, lateness, session, trigger, and output contract
- `jobs/` — job definitions
- `fixtures/events.jsonl` — out-of-order and late events for two keys
