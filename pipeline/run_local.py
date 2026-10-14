"""
Runs the whole pipeline once without Airflow, against SQLite by default.
Useful for development on a machine without Docker:

    python -m pipeline.run_local                 # today
    python -m pipeline.run_local --date 2026-10-01
"""

import argparse
import logging
import time
from datetime import date, timedelta

from . import storage, tasks
from .sources import SOURCES


def main():
    parser = argparse.ArgumentParser()
    # Default to yesterday: a full day of observed weather is available.
    parser.add_argument("--date", type=date.fromisoformat, default=date.today() - timedelta(days=1))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    run_id = f"local__{args.date.isoformat()}"
    engine = storage.get_engine()
    failures = []
    for source in SOURCES:
        started = time.time()
        try:
            tasks.extract(source, args.date)
            summary = tasks.validate(source, args.date)
            rows = tasks.load(source, args.date, run_id, summary, engine)
            print(f"OK    {source:17s} {rows:5d} rows  {summary['passed']}/{summary['evaluated']} checks  {time.time() - started:.1f}s")
        except Exception as error:  # report every source, then fail at the end
            failures.append(source)
            print(f"FAIL  {source:17s} {error}")

    print("Warehouse row counts:", storage.row_counts(engine))
    if failures:
        raise SystemExit(f"{len(failures)} source(s) failed: {', '.join(failures)}")


if __name__ == "__main__":
    main()
