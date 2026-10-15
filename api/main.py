"""
Read-only REST API over the warehouse.

    uvicorn api.main:app --port 8000      # docs at /docs

Uses WAREHOUSE_URL (Postgres in Docker, SQLite locally), same as the pipeline.
"""

from fastapi import FastAPI, Query
from fastapi.responses import RedirectResponse

from pipeline import queries, storage

app = FastAPI(
    title="Kenya Daily Data API",
    description="Validated weather, exchange rates, earthquakes and World Bank indicators for Kenya, refreshed daily.",
    version="1.0.0",
)


_ready: set[str] = set()


def engine():
    # Looked up per request so tests can point WAREHOUSE_URL at a temporary
    # database. Tables are created on first use so a fresh warehouse returns
    # empty lists instead of errors before the first pipeline run.
    eng = storage.get_engine()
    if str(eng.url) not in _ready:
        storage.ensure_tables(eng)
        _ready.add(str(eng.url))
    return eng


def records(df):
    return df.where(df.notna(), None).to_dict("records")


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse("/docs")


@app.get("/health")
def health():
    return {"status": "ok", **queries.summary(engine())}


@app.get("/weather")
def weather(city: str | None = Query(None, examples=["Nairobi"]), days: int = Query(30, ge=1, le=365)):
    return records(queries.weather(engine(), city, days))


@app.get("/fx/{currency}")
def fx(currency: str):
    return records(queries.fx_history(engine(), currency))


@app.get("/earthquakes")
def earthquakes(min_magnitude: float = Query(0.0, ge=0, le=10)):
    return records(queries.earthquakes(engine(), min_magnitude))


@app.get("/indicators")
def indicators():
    return records(queries.indicators(engine()))


@app.get("/runs")
def runs(limit: int = Query(40, ge=1, le=500)):
    return records(queries.runs(engine(), limit))
