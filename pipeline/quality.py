"""
Great Expectations checks. Each source has a suite; if any expectation fails,
validate() raises DataQualityError, which fails the Airflow task, so bad data
never reaches the warehouse.
"""

import logging
import os

os.environ.setdefault("GX_ANALYTICS_ENABLED", "false")

import great_expectations as gx  # noqa: E402
import great_expectations.expectations as gxe  # noqa: E402

log = logging.getLogger(__name__)


class DataQualityError(Exception):
    pass


def _suites():
    return {
        "weather": [
            gxe.ExpectTableRowCountToBeBetween(min_value=3, max_value=3),
            gxe.ExpectCompoundColumnsToBeUnique(column_list=["city", "date"]),
            *[gxe.ExpectColumnValuesToNotBeNull(column=c) for c in ["city", "date", "temp_max_c", "temp_min_c", "precipitation_mm"]],
            # Kenyan cities: plausible physical ranges, not climate records.
            gxe.ExpectColumnValuesToBeBetween(column="temp_max_c", min_value=0, max_value=50),
            gxe.ExpectColumnValuesToBeBetween(column="temp_min_c", min_value=-5, max_value=40),
            gxe.ExpectColumnPairValuesAToBeGreaterThanB(column_A="temp_max_c", column_B="temp_min_c", or_equal=True),
            gxe.ExpectColumnValuesToBeBetween(column="precipitation_mm", min_value=0, max_value=500),
        ],
        "fx_rates": [
            gxe.ExpectTableRowCountToBeBetween(min_value=100),
            gxe.ExpectColumnValuesToBeUnique(column="currency"),
            gxe.ExpectColumnValuesToNotBeNull(column="rate"),
            gxe.ExpectColumnValuesToBeBetween(column="rate", min_value=0, strict_min=True),
            gxe.ExpectColumnValuesToMatchRegex(column="currency", regex=r"^[A-Z]{3}$"),
            # The rate we care about most must be present.
            gxe.ExpectColumnDistinctValuesToContainSet(column="currency", value_set=["KES", "UGX", "TZS", "EUR"]),
        ],
        "earthquakes": [
            # Zero quakes in a week is a valid answer, so no minimum row count.
            gxe.ExpectColumnValuesToBeUnique(column="event_id"),
            gxe.ExpectColumnValuesToNotBeNull(column="event_time"),
            gxe.ExpectColumnValuesToBeBetween(column="magnitude", min_value=-1, max_value=10, mostly=0.99),
            gxe.ExpectColumnValuesToBeBetween(column="latitude", min_value=-12, max_value=15),
            gxe.ExpectColumnValuesToBeBetween(column="longitude", min_value=21, max_value=52),
        ],
        "kenya_indicators": [
            gxe.ExpectTableRowCountToBeBetween(min_value=3),
            gxe.ExpectCompoundColumnsToBeUnique(column_list=["indicator", "year"]),
            gxe.ExpectColumnValuesToBeInSet(column="indicator", value_set=["inflation_cpi_pct", "gdp_current_usd", "population"]),
            gxe.ExpectColumnValuesToBeBetween(column="year", min_value=1960, max_value=2100),
            gxe.ExpectColumnValuesToNotBeNull(column="value"),
        ],
    }


def validate(source: str, df):
    """Runs the source's suite on df. Returns a summary dict, or raises DataQualityError."""
    context = gx.get_context(mode="ephemeral")
    batch_definition = (
        context.data_sources.add_pandas(f"{source}_source")
        .add_dataframe_asset(f"{source}_asset")
        .add_batch_definition_whole_dataframe(f"{source}_batch")
    )
    suite = context.suites.add(gx.ExpectationSuite(name=f"{source}_suite"))
    for expectation in _suites()[source]:
        suite.add_expectation(expectation)

    result = batch_definition.get_batch(batch_parameters={"dataframe": df}).validate(suite)
    failures = [
        f"{r.expectation_config.type}({r.expectation_config.kwargs.get('column') or r.expectation_config.kwargs.get('column_list') or ''})"
        f" unexpected={r.result.get('unexpected_count', r.result.get('observed_value'))}"
        for r in result.results
        if not r.success
    ]
    summary = {
        "source": source,
        "rows": int(len(df)),
        "evaluated": result.statistics["evaluated_expectations"],
        "passed": result.statistics["successful_expectations"],
        "failures": failures,
    }
    log.info("Data quality %s: %s/%s expectations passed on %s rows", source, summary["passed"], summary["evaluated"], summary["rows"])
    if failures:
        raise DataQualityError(f"{source} failed {len(failures)} check(s): " + "; ".join(failures))
    return summary
