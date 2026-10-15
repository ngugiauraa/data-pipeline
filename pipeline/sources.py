"""
The four keyless public APIs, each split into:
    fetch_*      download raw JSON (network)
    transform_*  turn that JSON into a tidy DataFrame (pure, unit-tested)

None of these APIs needs a key or an account.
"""

from datetime import date, timedelta

import pandas as pd
import requests

TIMEOUT = 30
HEADERS = {"User-Agent": "kenya-data-pipeline/1.0 (portfolio project)"}

CITIES = {
    "Nairobi": (-1.2864, 36.8172),
    "Mombasa": (-4.0435, 39.6682),
    "Kisumu": (-0.0917, 34.7680),
}
# East African Rift and surroundings, for the earthquake feed.
EAST_AFRICA_BBOX = {"minlatitude": -12, "maxlatitude": 15, "minlongitude": 21, "maxlongitude": 52}
WORLD_BANK_INDICATORS = {
    "FP.CPI.TOTL.ZG": "inflation_cpi_pct",
    "NY.GDP.MKTP.CD": "gdp_current_usd",
    "SP.POP.TOTL": "population",
}


def _get(url, params=None):
    response = requests.get(url, params=params, headers=HEADERS, timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()


# 1. Open-Meteo: daily weather ------------------------------------------------

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
# The forecast endpoint only keeps recent days; older dates come back as nulls
# (caught by the data-quality checks during the first backfill). The archive
# has full history but can lag a few days, so it is used for older dates only.
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
ARCHIVE_AFTER_DAYS = 7


def weather_url(run_date: date, today: date | None = None) -> str:
    today = today or date.today()
    return ARCHIVE_URL if (today - run_date).days > ARCHIVE_AFTER_DAYS else FORECAST_URL


def fetch_weather(run_date: date):
    """Observed weather for run_date for each city."""
    payload = {}
    url = weather_url(run_date)
    for city, (lat, lon) in CITIES.items():
        payload[city] = _get(
            url,
            {
                "latitude": lat,
                "longitude": lon,
                "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
                "timezone": "Africa/Nairobi",
                "start_date": run_date.isoformat(),
                "end_date": run_date.isoformat(),
            },
        )
    return payload


def transform_weather(payload) -> pd.DataFrame:
    rows = []
    for city, body in payload.items():
        daily = body["daily"]
        for i, day in enumerate(daily["time"]):
            rows.append({
                "city": city,
                "date": day,
                "temp_max_c": daily["temperature_2m_max"][i],
                "temp_min_c": daily["temperature_2m_min"][i],
                "precipitation_mm": daily["precipitation_sum"][i],
            })
    return pd.DataFrame(rows, columns=["city", "date", "temp_max_c", "temp_min_c", "precipitation_mm"])


# 2. ExchangeRate-API (open access): currency rates -----------------------------

def fetch_fx(_run_date: date):
    return _get("https://open.er-api.com/v6/latest/USD")


def transform_fx(payload) -> pd.DataFrame:
    if payload.get("result") != "success":
        raise ValueError(f"FX API returned result={payload.get('result')!r}")
    rate_date = pd.to_datetime(payload["time_last_update_unix"], unit="s", utc=True).date().isoformat()
    rows = [
        {"base": payload["base_code"], "currency": code, "rate": float(rate), "rate_date": rate_date}
        for code, rate in payload["rates"].items()
    ]
    return pd.DataFrame(rows, columns=["base", "currency", "rate", "rate_date"])


# 3. USGS: earthquakes in East Africa --------------------------------------------

def fetch_earthquakes(run_date: date):
    return _get(
        "https://earthquake.usgs.gov/fdsnws/event/1/query",
        {
            "format": "geojson",
            "starttime": (run_date - timedelta(days=7)).isoformat(),
            "endtime": (run_date + timedelta(days=1)).isoformat(),
            **EAST_AFRICA_BBOX,
        },
    )


def transform_earthquakes(payload) -> pd.DataFrame:
    rows = []
    for feature in payload.get("features", []):
        props = feature["properties"]
        lon, lat, depth = feature["geometry"]["coordinates"][:3]
        rows.append({
            "event_id": feature["id"],
            "event_time": pd.to_datetime(props["time"], unit="ms", utc=True).isoformat(),
            "magnitude": props.get("mag"),
            "place": props.get("place"),
            "latitude": lat,
            "longitude": lon,
            "depth_km": depth,
        })
    return pd.DataFrame(rows, columns=["event_id", "event_time", "magnitude", "place", "latitude", "longitude", "depth_km"])


# 4. World Bank: Kenya indicators ---------------------------------------------------

def fetch_world_bank(_run_date: date):
    return {
        code: _get(f"https://api.worldbank.org/v2/country/KEN/indicator/{code}", {"format": "json", "per_page": 20})
        for code in WORLD_BANK_INDICATORS
    }


def transform_world_bank(payload) -> pd.DataFrame:
    rows = []
    for code, body in payload.items():
        # The API returns [paging_info, records]; records is null for unknown indicators.
        records = body[1] if isinstance(body, list) and len(body) > 1 and body[1] else []
        for record in records:
            if record.get("value") is None:
                continue  # the latest years are often not published yet
            rows.append({
                "country": record["countryiso3code"],
                "indicator": WORLD_BANK_INDICATORS[code],
                "year": int(record["date"]),
                "value": float(record["value"]),
            })
    return pd.DataFrame(rows, columns=["country", "indicator", "year", "value"])


SOURCES = {
    "weather": (fetch_weather, transform_weather),
    "fx_rates": (fetch_fx, transform_fx),
    "earthquakes": (fetch_earthquakes, transform_earthquakes),
    "kenya_indicators": (fetch_world_bank, transform_world_bank),
}
