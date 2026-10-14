"""
Daily pipeline: four keyless public APIs -> landing zone (raw JSON) ->
Great Expectations checks -> Postgres warehouse.

For each source: extract >> validate >> load. Sources run in parallel and
fail independently: a bad FX response never blocks the weather load. A final
report task summarises the run and fails it if any source failed.

The logic lives in the `pipeline` package so it can be tested without Airflow.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

from pipeline import storage, tasks
from pipeline.sources import SOURCES

DEFAULT_ARGS = {
    "owner": "data",
    # Public APIs have the odd bad minute; retry with growing waits.
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=10),
    # Each run collects the previous full day (data_interval_start), so 07:00
    # Nairobi runs always see a complete day of observed weather.
}


@task(trigger_rule=TriggerRule.ALL_DONE, retries=0)
def report():
    """Logs warehouse row counts and fails the run if any upstream task failed."""
    import logging

    context = get_current_context()
    counts = storage.row_counts(storage.get_engine())
    logging.getLogger(__name__).info("Warehouse row counts after run %s: %s", context["run_id"], counts)
    failed = [ti.task_id for ti in context["dag_run"].get_task_instances() if ti.state == "failed"]
    if failed:
        raise RuntimeError(f"Failed tasks: {', '.join(failed)}")
    return counts


@dag(
    dag_id="kenya_daily_pipeline",
    description="Weather, FX, earthquakes and World Bank indicators for Kenya, validated with Great Expectations",
    schedule="0 4 * * *",  # 07:00 Nairobi time
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["portfolio", "kenya", "great-expectations"],
)
def kenya_daily_pipeline():
    final_report = report()

    for source in SOURCES:

        @task(task_id=f"extract_{source}")
        def extract(source=source):
            return tasks.extract(source, get_current_context()["data_interval_start"].date())

        # Data-quality failures are not transient, so this task does not retry.
        @task(task_id=f"validate_{source}", retries=0)
        def validate(_raw_path, source=source):
            return tasks.validate(source, get_current_context()["data_interval_start"].date())

        @task(task_id=f"load_{source}")
        def load(summary, source=source):
            context = get_current_context()
            return tasks.load(source, context["data_interval_start"].date(), context["run_id"], summary)

        load(validate(extract())) >> final_report


kenya_daily_pipeline()
