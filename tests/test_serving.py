"""Tests for the read side: shared queries and the REST API, on a seeded SQLite warehouse."""

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from pipeline import queries, storage


@pytest.fixture
def warehouse(tmp_path, monkeypatch):
    url = f"sqlite:///{(tmp_path / 'w.db').as_posix()}"
    monkeypatch.setenv("WAREHOUSE_URL", url)
    engine = storage.get_engine(url)
    storage.ensure_tables(engine)
    storage.upsert(engine, "weather", pd.DataFrame([
        {"city": "Nairobi", "date": "2026-10-12", "temp_max_c": 24.0, "temp_min_c": 13.0, "precipitation_mm": 0.0},
        {"city": "Nairobi", "date": "2026-10-13", "temp_max_c": 25.0, "temp_min_c": 14.0, "precipitation_mm": 2.5},
        {"city": "Mombasa", "date": "2026-10-13", "temp_max_c": 31.0, "temp_min_c": 24.0, "precipitation_mm": 0.0},
    ]), "t")
    storage.upsert(engine, "fx_rates", pd.DataFrame([
        {"base": "USD", "currency": "KES", "rate": 129.1, "rate_date": "2026-10-13"},
        {"base": "USD", "currency": "KES", "rate": 129.6, "rate_date": "2026-10-14"},
        {"base": "USD", "currency": "EUR", "rate": 0.92, "rate_date": "2026-10-14"},
    ]), "t")
    storage.upsert(engine, "earthquakes", pd.DataFrame([
        {"event_id": "a", "event_time": "2026-10-10T01:00:00+00:00", "magnitude": 4.6, "place": "Lake Tanganyika",
         "latitude": -6.1, "longitude": 29.5, "depth_km": 10.0},
        {"event_id": "b", "event_time": "2026-10-11T01:00:00+00:00", "magnitude": 2.1, "place": "Rift Valley",
         "latitude": 0.5, "longitude": 36.0, "depth_km": 8.0},
    ]), "t")
    storage.upsert(engine, "kenya_indicators", pd.DataFrame([
        {"country": "KEN", "indicator": "inflation_cpi_pct", "year": 2025, "value": 4.07},
    ]), "t")
    storage.upsert(engine, "pipeline_runs", pd.DataFrame([
        {"run_id": "r1", "source": "weather", "run_date": "2026-10-13", "rows_loaded": 3, "checks_passed": 11,
         "checks_evaluated": 11},
    ]), "2026-10-14T04:00:00")
    return engine


def test_queries(warehouse):
    assert list(queries.weather(warehouse, city="nairobi")["temp_max_c"]) == [24.0, 25.0]
    assert list(queries.fx_history(warehouse, "kes")["rate"]) == [129.1, 129.6]
    assert list(queries.earthquakes(warehouse, min_magnitude=4)["event_id"]) == ["a"]
    summary = queries.summary(warehouse)
    assert summary["usd_kes"] == 129.6
    assert summary["weather_days"] == 2
    assert summary["last_run_checks"] == "11/11"


def test_api(warehouse):
    from api.main import app

    client = TestClient(app)
    assert client.get("/health").json()["usd_kes"] == 129.6
    assert len(client.get("/weather", params={"city": "Mombasa"}).json()) == 1
    assert client.get("/fx/KES").json()[-1]["rate"] == 129.6
    assert client.get("/earthquakes", params={"min_magnitude": 4}).json()[0]["place"] == "Lake Tanganyika"
    assert client.get("/indicators").json()[0]["indicator"] == "inflation_cpi_pct"
    assert client.get("/runs").json()[0]["checks_passed"] == 11
    assert client.get("/weather", params={"days": 0}).status_code == 422
