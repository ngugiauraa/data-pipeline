"""Offline tests: transforms, data-quality gates and idempotent loading. No network."""

import pandas as pd
import pytest

from pipeline import quality, storage
from pipeline.sources import transform_earthquakes, transform_fx, transform_weather, transform_world_bank

WEATHER = {
    city: {"daily": {"time": ["2026-10-13"], "temperature_2m_max": [tmax], "temperature_2m_min": [tmin], "precipitation_sum": [0.4]}}
    for city, tmax, tmin in [("Nairobi", 24.1, 13.2), ("Mombasa", 30.5, 23.9), ("Kisumu", 28.7, 17.0)]
}
FX = {
    "result": "success",
    "base_code": "USD",
    "time_last_update_unix": 1791936152,
    "rates": {"USD": 1, "KES": 129.2, "UGX": 3700.5, "TZS": 2600.1, "EUR": 0.92, **{f"Q{chr(65 + i // 26)}{chr(65 + i % 26)}": 1.0 + i for i in range(100)}},
}
QUAKES = {
    "features": [
        {"id": "us7000abcd", "properties": {"time": 1791900000000, "mag": 4.6, "place": "Lake Tanganyika"}, "geometry": {"coordinates": [29.5, -6.1, 10.0]}},
    ]
}
WORLD_BANK = {
    "FP.CPI.TOTL.ZG": [{"page": 1}, [{"countryiso3code": "KEN", "date": "2025", "value": 4.07}, {"countryiso3code": "KEN", "date": "2026", "value": None}]],
    "NY.GDP.MKTP.CD": [{"page": 1}, [{"countryiso3code": "KEN", "date": "2025", "value": 1.2e11}]],
    "SP.POP.TOTL": [{"page": 1}, [{"countryiso3code": "KEN", "date": "2025", "value": 5.6e7}]],
}


def test_transforms_shape_the_raw_json():
    weather = transform_weather(WEATHER)
    assert list(weather.city) == ["Nairobi", "Mombasa", "Kisumu"]
    fx = transform_fx(FX)
    assert fx.loc[fx.currency == "KES", "rate"].item() == 129.2
    assert fx.rate_date.unique().tolist() == ["2026-10-14"]
    quakes = transform_earthquakes(QUAKES)
    assert quakes.iloc[0].to_dict()["latitude"] == -6.1
    indicators = transform_world_bank(WORLD_BANK)
    assert len(indicators) == 3  # the unpublished 2026 value is skipped
    assert transform_earthquakes({"features": []}).empty


def test_fx_transform_rejects_an_api_error():
    with pytest.raises(ValueError):
        transform_fx({"result": "error"})


def test_good_data_passes_every_suite():
    for source, df in [
        ("weather", transform_weather(WEATHER)),
        ("fx_rates", transform_fx(FX)),
        ("earthquakes", transform_earthquakes(QUAKES)),
        ("earthquakes", transform_earthquakes({"features": []})),
        ("kenya_indicators", transform_world_bank(WORLD_BANK)),
    ]:
        summary = quality.validate(source, df)
        assert summary["failures"] == []


@pytest.mark.parametrize(
    "mutate",
    [
        lambda df: df.assign(temp_max_c=[24.1, 99.0, 28.7]),  # impossible temperature
        lambda df: df.assign(temp_min_c=[30.0, 23.9, 17.0]),  # min above max
        lambda df: df.iloc[:2],  # a city is missing
        lambda df: pd.concat([df, df.iloc[[0]]]),  # duplicate row
        lambda df: df.assign(precipitation_mm=[None, 0.0, 0.0]),  # missing value
    ],
)
def test_bad_weather_fails_the_run(mutate):
    with pytest.raises(quality.DataQualityError):
        quality.validate("weather", mutate(transform_weather(WEATHER)))


def test_missing_kes_rate_fails_the_run():
    fx = transform_fx(FX)
    with pytest.raises(quality.DataQualityError, match="fx_rates"):
        quality.validate("fx_rates", fx[fx.currency != "KES"])


def test_loads_are_idempotent(tmp_path):
    engine = storage.get_engine(f"sqlite:///{(tmp_path / 'w.db').as_posix()}")
    storage.ensure_tables(engine)
    weather = transform_weather(WEATHER)
    storage.upsert(engine, "weather", weather, "t1")
    storage.upsert(engine, "weather", weather.assign(temp_max_c=weather.temp_max_c + 1), "t2")
    assert storage.row_counts(engine)["weather"] == 3
    with engine.connect() as conn:
        latest = pd.read_sql("select temp_max_c, loaded_at from weather where city = 'Nairobi'", conn)
    assert latest.iloc[0].to_dict() == {"temp_max_c": 25.1, "loaded_at": "t2"}
    assert storage.upsert(engine, "earthquakes", transform_earthquakes({"features": []}), "t3") == 0
