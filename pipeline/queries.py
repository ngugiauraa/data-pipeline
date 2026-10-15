"""
Read-side queries over the warehouse, shared by the dashboard and the API so
both always show the same numbers. Plain SQL that works on Postgres and SQLite.
"""

import pandas as pd
from sqlalchemy import text


def _df(engine, sql, **params) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def weather(engine, city: str | None = None, days: int = 90) -> pd.DataFrame:
    sql = "select city, date, temp_max_c, temp_min_c, precipitation_mm from weather"
    if city:
        sql += " where lower(city) = lower(:city)"
    sql += " order by date desc, city"
    df = _df(engine, sql, city=city)
    if df.empty:
        return df
    latest = pd.to_datetime(df["date"]).max()
    return df[pd.to_datetime(df["date"]) > latest - pd.Timedelta(days=days)].sort_values(["date", "city"])


def fx_history(engine, currency: str = "KES") -> pd.DataFrame:
    """One USD rate per day for a currency (the daily run stores today's rate)."""
    return _df(
        engine,
        "select rate_date, rate from fx_rates where currency = :currency and base = 'USD' order by rate_date",
        currency=currency.upper(),
    )


def earthquakes(engine, min_magnitude: float = 0.0) -> pd.DataFrame:
    return _df(
        engine,
        "select event_id, event_time, magnitude, place, latitude, longitude, depth_km from earthquakes "
        "where coalesce(magnitude, 0) >= :m order by event_time desc",
        m=min_magnitude,
    )


def indicators(engine) -> pd.DataFrame:
    return _df(engine, "select indicator, year, value from kenya_indicators order by indicator, year")


def runs(engine, limit: int = 60) -> pd.DataFrame:
    """Most recent run per source per day, newest first: the data-quality history."""
    return _df(
        engine,
        "select run_date, source, rows_loaded, checks_passed, checks_evaluated, loaded_at from pipeline_runs "
        "order by loaded_at desc limit :limit",
        limit=limit,
    )


def summary(engine) -> dict:
    """Headline numbers for the dashboard and the API root."""
    fx = fx_history(engine)
    # All sources of the most recent run (one run_id covers every source loaded that day).
    last_runs = _df(
        engine,
        "select loaded_at, checks_passed, checks_evaluated from pipeline_runs where run_id = "
        "(select run_id from pipeline_runs order by loaded_at desc limit 1) order by loaded_at desc",
    )
    with engine.connect() as conn:
        days = conn.execute(text("select count(distinct date) from weather")).scalar_one()
        quakes = conn.execute(text("select count(*) from earthquakes")).scalar_one()
    return {
        "usd_kes": float(fx["rate"].iloc[-1]) if not fx.empty else None,
        "usd_kes_date": fx["rate_date"].iloc[-1] if not fx.empty else None,
        "weather_days": int(days),
        "earthquakes": int(quakes),
        "last_run": str(last_runs["loaded_at"].iloc[0]) if not last_runs.empty else None,
        "last_run_checks": (
            f"{int(last_runs['checks_passed'].sum())}/{int(last_runs['checks_evaluated'].sum())}"
            if not last_runs.empty else None
        ),
    }
