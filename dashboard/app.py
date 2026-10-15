"""
Streamlit dashboard over the warehouse.

    streamlit run dashboard/app.py

Data source, in order:
  1. WAREHOUSE_URL (e.g. Postgres in Docker Compose)
  2. WAREHOUSE_DB_URL, a SQLite file to download (the daily GitHub Action
     publishes one to the repo's "data" release, which is what the public
     Streamlit Cloud deployment uses)
  3. the local SQLite file written by `python -m pipeline.run_local`
"""

import os
import sys
import tempfile
import urllib.request

import altair as alt
import pandas as pd
import streamlit as st

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))  # so "pipeline" imports on Streamlit Cloud

from pipeline import queries, storage  # noqa: E402

RELEASE_DB = "https://github.com/ngugiauraa/data-pipeline/releases/download/data/warehouse.db"
LOCAL_DB = os.path.join(os.path.dirname(__file__), "..", "data", "warehouse.db")

st.set_page_config(page_title="Kenya Daily Data", page_icon="🇰🇪", layout="wide")


@st.cache_resource(ttl=3600)
def get_engine():
    if os.environ.get("WAREHOUSE_URL"):
        return storage.get_engine(), "Postgres warehouse"
    if os.path.exists(LOCAL_DB) and not os.environ.get("WAREHOUSE_DB_URL"):
        return storage.get_engine(), "local SQLite warehouse"
    url = os.environ.get("WAREHOUSE_DB_URL", RELEASE_DB)
    path = os.path.join(tempfile.gettempdir(), "kenya-warehouse.db")
    urllib.request.urlretrieve(url, path)
    return storage.get_engine("sqlite:///" + path.replace("\\", "/")), "latest daily run (GitHub release)"


engine, source = get_engine()
storage.ensure_tables(engine)  # a brand-new warehouse shows empty charts instead of an error
summary = queries.summary(engine)

st.title("Kenya Daily Data")
st.caption(
    f"Weather, exchange rates, earthquakes and World Bank indicators, collected daily by an Airflow pipeline "
    f"and checked with Great Expectations before loading. Source: {source}. "
    f"Code: github.com/ngugiauraa/data-pipeline"
)

c1, c2, c3, c4 = st.columns(4)
c1.metric("USD → KES", f"{summary['usd_kes']:.2f}" if summary["usd_kes"] else "–", help=f"Rate date {summary['usd_kes_date']}")
c2.metric("Days of weather", summary["weather_days"])
c3.metric("Earthquakes (East Africa)", summary["earthquakes"])
c4.metric("Last run checks passed", summary["last_run_checks"] or "–", help=f"Last load {summary['last_run']}")

st.subheader("Daily weather")
weather = queries.weather(engine, days=120)
if weather.empty:
    st.info("No weather loaded yet.")
else:
    weather["date"] = pd.to_datetime(weather["date"])
    left, right = st.columns(2)
    left.altair_chart(
        alt.Chart(weather).mark_line(point=True).encode(
            x=alt.X("date:T", title=None),
            y=alt.Y("temp_max_c:Q", title="Max temperature (°C)"),
            color=alt.Color("city:N", title="City"),
            tooltip=["city", "date:T", "temp_max_c", "temp_min_c"],
        ).properties(height=300),
        use_container_width=True,
    )
    right.altair_chart(
        alt.Chart(weather).mark_bar().encode(
            x=alt.X("date:T", title=None),
            y=alt.Y("precipitation_mm:Q", title="Rainfall (mm)"),
            color=alt.Color("city:N", title="City"),
            tooltip=["city", "date:T", "precipitation_mm"],
        ).properties(height=300),
        use_container_width=True,
    )

st.subheader("US dollar to Kenyan shilling")
fx = queries.fx_history(engine)
if len(fx) < 2:
    st.info("The rate history grows by one point per daily run.")
if not fx.empty:
    fx["rate_date"] = pd.to_datetime(fx["rate_date"])
    st.altair_chart(
        alt.Chart(fx).mark_line(point=True).encode(
            x=alt.X("rate_date:T", title=None),
            y=alt.Y("rate:Q", title="KES per USD", scale=alt.Scale(zero=False)),
            tooltip=["rate_date:T", "rate"],
        ).properties(height=250),
        use_container_width=True,
    )

st.subheader("Earthquakes in the East African Rift")
quakes = queries.earthquakes(engine)
if quakes.empty:
    st.info("No earthquakes recorded in the collected windows.")
else:
    st.map(quakes.rename(columns={"latitude": "lat", "longitude": "lon"}), size="magnitude")
    st.dataframe(quakes[["event_time", "magnitude", "place", "depth_km"]], hide_index=True, use_container_width=True)

st.subheader("Kenya indicators (World Bank)")
ind = queries.indicators(engine)
if not ind.empty:
    labels = {"inflation_cpi_pct": "Inflation (%)", "gdp_current_usd": "GDP (current USD)", "population": "Population"}
    cols = st.columns(3)
    for col, (key, label) in zip(cols, labels.items(), strict=True):
        data = ind[ind["indicator"] == key]
        col.altair_chart(
            alt.Chart(data).mark_line().encode(x=alt.X("year:O", title=None), y=alt.Y("value:Q", title=label))
            .properties(height=220, title=label),
            use_container_width=True,
        )

st.subheader("Data-quality history")
st.caption("Every load records how many Great Expectations checks passed. A failed check stops that source from loading.")
st.dataframe(queries.runs(engine, limit=40), hide_index=True, use_container_width=True)
