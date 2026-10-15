"""
Runs the whole pipeline without Airflow, against SQLite by default.
Used for development and by the daily GitHub Actions run.

    python -m pipeline.run_local                       # yesterday
    python -m pipeline.run_local --date 2026-10-01
    python -m pipeline.run_local --backfill 30         # the last 30 days, oldest first

Exits non-zero if any source fails, so a scheduled run shows as failed.
"""

import argparse
import logging
import os
import time
from datetime import date, timedelta

from . import storage, tasks
from .sources import SOURCES

# Exchange rates and World Bank figures do not change per past day, so a
# backfill only re-reads the sources that have real daily history.
BACKFILL_SOURCES = ("weather", "earthquakes")


def run_day(day: date, engine, sources, run_prefix: str) -> list[str]:
    run_id = f"{run_prefix}__{day.isoformat()}"
    failures = []
    for source in sources:
        started = time.time()
        try:
            tasks.extract(source, day)
            summary = tasks.validate(source, day)
            rows = tasks.load(source, day, run_id, summary, engine)
            print(f"OK    {day} {source:17s} {rows:5d} rows  {summary['passed']}/{summary['evaluated']} checks  "
                  f"{time.time() - started:.1f}s")
        except Exception as error:  # report every source, then fail at the end
            failures.append(f"{day} {source}")
            print(f"FAIL  {day} {source:17s} {error}")
    return failures


def main():
    parser = argparse.ArgumentParser()
    # Default to yesterday: a full day of observed weather is available.
    parser.add_argument("--date", type=date.fromisoformat, default=date.today() - timedelta(days=1))
    parser.add_argument("--backfill", type=int, default=0, help="also load this many days before --date")
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    engine = storage.get_engine()
    run_prefix = os.environ.get("RUN_PREFIX", "local")
    failures = []
    for offset in range(args.backfill, 0, -1):
        failures += run_day(args.date - timedelta(days=offset), engine, BACKFILL_SOURCES, run_prefix)
    failures += run_day(args.date, engine, list(SOURCES), run_prefix)

    print("Warehouse row counts:", storage.row_counts(engine))
    if failures:
        raise SystemExit(f"{len(failures)} load(s) failed: {', '.join(failures)}")


if __name__ == "__main__":
    main()
