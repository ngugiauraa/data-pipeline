"""
The three steps of each source, as plain functions so the Airflow DAG and
the local runner call exactly the same code:

    extract   download raw JSON and save it unchanged to the landing zone
    validate  transform to a table and run Great Expectations (raises on failure)
    load      upsert the validated table into the warehouse
"""

import json
import logging
import os
from datetime import date, datetime, timezone

import pandas as pd

from . import quality, storage
from .sources import SOURCES

log = logging.getLogger(__name__)

LANDING_DIR = os.environ.get("LANDING_DIR", os.path.join(os.path.dirname(__file__), "..", "data", "landing"))


def _raw_path(source, run_date):
    return os.path.join(LANDING_DIR, source, f"{run_date.isoformat()}.json")


def _staged_path(source, run_date):
    return os.path.join(LANDING_DIR, source, f"{run_date.isoformat()}.validated.csv")


def extract(source: str, run_date: date) -> str:
    fetch, _ = SOURCES[source]
    payload = fetch(run_date)
    path = _raw_path(source, run_date)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    log.info("Extracted %s -> %s", source, path)
    return path


def validate(source: str, run_date: date) -> dict:
    _, transform = SOURCES[source]
    with open(_raw_path(source, run_date), encoding="utf-8") as f:
        df = transform(json.load(f))
    summary = quality.validate(source, df)  # raises DataQualityError on bad data
    df.to_csv(_staged_path(source, run_date), index=False)
    return summary


def load(source: str, run_date: date, run_id: str, summary: dict, engine=None) -> int:
    engine = engine or storage.get_engine()
    storage.ensure_tables(engine)
    path = _staged_path(source, run_date)
    df = pd.read_csv(path, keep_default_na=True) if os.path.getsize(path) > 1 else pd.DataFrame()
    loaded_at = datetime.now(timezone.utc).isoformat()
    rows = storage.upsert(engine, source, df, loaded_at)
    storage.upsert(
        engine,
        "pipeline_runs",
        pd.DataFrame([{
            "run_id": run_id,
            "source": source,
            "run_date": run_date.isoformat(),
            "rows_loaded": rows,
            "checks_passed": summary["passed"],
            "checks_evaluated": summary["evaluated"],
        }]),
        loaded_at,
    )
    return rows
