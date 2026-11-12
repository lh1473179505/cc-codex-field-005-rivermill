"""CLI: inspect a job+input, or run the engine (run is the work to implement)."""

from __future__ import annotations

import argparse
import sys

from .engine import run_job
from .errors import NotImplementedEngine, RivermillError
from .events import iter_jsonl
from .job import load_job


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="rivermill")
    sub = p.add_subparsers(dest="cmd", required=True)

    ins = sub.add_parser("inspect", help="summarize a job and its input events")
    ins.add_argument("--job", required=True)
    ins.add_argument("--input", required=True)

    run = sub.add_parser("run", help="execute the window job")
    run.add_argument("--job", required=True)
    run.add_argument("--input", required=True)
    run.add_argument("--out", required=True, help="output directory")
    run.add_argument("--checkpoint", default=None, help="path to write the final checkpoint")
    run.add_argument("--from-checkpoint", default=None, dest="from_checkpoint", help="resume from this checkpoint")
    return p


def main(argv: list[str] | None = None) -> int:
    p = build_parser()
    args = p.parse_args(argv)
    try:
        job = load_job(args.job)
        if args.cmd == "inspect":
            n = 0
            min_ts = max_ts = None
            keys = set()
            for ev in iter_jsonl(args.input, job.event_time_field, job.key_fields):
                n += 1
                min_ts = ev.ts if min_ts is None else min(min_ts, ev.ts)
                max_ts = ev.ts if max_ts is None else max(max_ts, ev.ts)
                keys.add(ev.key)
            print(f"job {job.name}")
            print(f"event_time_field {job.event_time_field}")
            print(f"key_fields {','.join(job.key_fields)}")
            print(f"lag_ms {job.watermark.lag_ms} allowed_lateness_ms {job.allowed_lateness_ms} idle_eof {job.watermark.idle_eof}")
            print(f"windows {len(job.windows)}: " + ", ".join(f"{w.name}({w.type})" for w in job.windows))
            print(f"events {n} keys {len(keys)} ts_min {min_ts} ts_max {max_ts}")
            return 0
        if args.cmd == "run":
            run_job(
                job,
                input_path=args.input,
                out_dir=args.out,
                checkpoint_path=args.checkpoint,
                from_checkpoint=args.from_checkpoint,
            )
            return 0
        p.error("unknown command")
        return 2
    except FileNotFoundError as e:
        print(f"error: file not found: {e.filename or e}", file=sys.stderr)
        return 2
    except NotImplementedEngine as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    except RivermillError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
