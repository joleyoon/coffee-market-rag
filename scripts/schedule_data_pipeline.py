#!/usr/bin/env python3
"""Run the data pipeline on a simple local schedule."""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_data_pipeline import build_parser as build_pipeline_parser
from scripts.run_data_pipeline import run_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--interval-hours",
        type=float,
        default=24.0,
        help="Run the pipeline every N hours when --daily-at is not set.",
    )
    parser.add_argument(
        "--daily-at",
        default=None,
        help="Run once per day at local time HH:MM, for example 06:30.",
    )
    parser.add_argument("--run-once", action="store_true")
    parser.add_argument(
        "--max-runs",
        type=int,
        default=None,
        help="Stop after this many scheduled runs.",
    )
    parser.add_argument(
        "--wait-first",
        action="store_true",
        help="Wait for the first schedule boundary before running.",
    )
    args, pipeline_argv = parser.parse_known_args()
    pipeline_args = build_pipeline_parser().parse_args(pipeline_argv)
    args.pipeline_args = pipeline_args
    return args


def parse_daily_time(value: str) -> tuple[int, int]:
    hour_text, minute_text = value.split(":", 1)
    hour = int(hour_text)
    minute = int(minute_text)
    if not 0 <= hour <= 23 or not 0 <= minute <= 59:
        raise ValueError("daily time must be in HH:MM 24-hour format")
    return hour, minute


def seconds_until_next_run(interval_hours: float, daily_at: str | None) -> float:
    now = datetime.now()
    if daily_at:
        hour, minute = parse_daily_time(daily_at)
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        return max((target - now).total_seconds(), 0.0)
    return max(interval_hours * 3600.0, 0.0)


def main() -> int:
    args = parse_args()
    completed_runs = 0
    run_immediately = not args.wait_first

    while True:
        if not run_immediately:
            sleep_seconds = seconds_until_next_run(args.interval_hours, args.daily_at)
            next_run = datetime.now() + timedelta(seconds=sleep_seconds)
            print(f"[scheduler] sleeping until {next_run.isoformat(timespec='seconds')}")
            time.sleep(sleep_seconds)

        print(f"[scheduler] starting pipeline run #{completed_runs + 1}")
        exit_code = 0
        try:
            _, failures = run_pipeline(args.pipeline_args)
            exit_code = 1 if failures else 0
        except Exception as exc:  # noqa: BLE001
            print(f"[scheduler] pipeline failed: {exc}", file=sys.stderr)
            exit_code = 1
        completed_runs += 1
        run_immediately = False

        if args.run_once:
            return exit_code
        if args.max_runs is not None and completed_runs >= args.max_runs:
            return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
