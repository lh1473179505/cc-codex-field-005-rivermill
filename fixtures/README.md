# Fixtures

`events.jsonl` is a short synthetic stream for two shops (`A` and `B`). Timestamps are small epoch-style milliseconds. Arrival order is intentionally not sorted:

- Several shop A events in `[0, 10000)` and `[10000, 20000)` plus later events
- An out-of-order shop A event at ts=2500 that arrives after ts=4000
- A late-but-admitted shop A event at ts=2000 that arrives after the first tumbling window should have closed
- A too-late shop A event at ts=500 that should go to the late side output
- Shop B events that form a different key and session layout

The job in `jobs/shop_windows.json` uses lag 5s, allowed lateness 10s, a 10s tumbling window, a 15s/5s sliding window, and an 8s-gap session window.
