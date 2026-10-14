"""
Warehouse storage. Works with Postgres (in Docker) or SQLite (local runs and
tests), chosen by the WAREHOUSE_URL environment variable.

Loads are idempotent: rows are upserted on each table's natural key, so
re-running a day (an Airflow retry or backfill) never creates duplicates.
"""

import logging
import os

from sqlalchemy import create_engine, text

log = logging.getLogger(__name__)

DEFAULT_URL = "sqlite:///" + os.path.join(os.path.dirname(__file__), "..", "data", "warehouse.db").replace("\\", "/")

TABLES = {
    "weather": {
        "key": ["city", "date"],
        "ddl": """create table if not exists weather (
            city text not null, date text not null,
            temp_max_c double precision, temp_min_c double precision, precipitation_mm double precision,
            loaded_at text not null, primary key (city, date))""",
    },
    "fx_rates": {
        "key": ["base", "currency", "rate_date"],
        "ddl": """create table if not exists fx_rates (
            base text not null, currency text not null, rate double precision not null, rate_date text not null,
            loaded_at text not null, primary key (base, currency, rate_date))""",
    },
    "earthquakes": {
        "key": ["event_id"],
        "ddl": """create table if not exists earthquakes (
            event_id text primary key, event_time text not null, magnitude double precision, place text,
            latitude double precision, longitude double precision, depth_km double precision, loaded_at text not null)""",
    },
    "kenya_indicators": {
        "key": ["country", "indicator", "year"],
        "ddl": """create table if not exists kenya_indicators (
            country text not null, indicator text not null, year integer not null, value double precision not null,
            loaded_at text not null, primary key (country, indicator, year))""",
    },
    "pipeline_runs": {
        "key": ["run_id", "source"],
        "ddl": """create table if not exists pipeline_runs (
            run_id text not null, source text not null, run_date text not null, rows_loaded integer not null,
            checks_passed integer not null, checks_evaluated integer not null, loaded_at text not null,
            primary key (run_id, source))""",
    },
}


def get_engine(url=None):
    return create_engine(url or os.environ.get("WAREHOUSE_URL", DEFAULT_URL), future=True)


def ensure_tables(engine):
    with engine.begin() as conn:
        for spec in TABLES.values():
            conn.execute(text(spec["ddl"]))


def upsert(engine, table, df, loaded_at):
    """Insert or update every row of df by the table's key. Returns rows written."""
    if df.empty:
        return 0
    spec = TABLES[table]
    records = df.assign(loaded_at=loaded_at).to_dict("records")
    columns = list(records[0].keys())
    updates = [c for c in columns if c not in spec["key"]]
    # Same ON CONFLICT syntax works in Postgres and SQLite 3.24+.
    statement = text(
        f"insert into {table} ({', '.join(columns)}) values ({', '.join(':' + c for c in columns)}) "
        f"on conflict ({', '.join(spec['key'])}) do update set "
        + ", ".join(f"{c} = excluded.{c}" for c in updates)
    )
    with engine.begin() as conn:
        conn.execute(statement, records)
    log.info("Upserted %s rows into %s", len(records), table)
    return len(records)


def row_counts(engine):
    with engine.connect() as conn:
        return {t: conn.execute(text(f"select count(*) from {t}")).scalar_one() for t in TABLES}
