from __future__ import annotations

from prefect import flow, get_run_logger, task

from scripts.run_pipeline import DEFAULT_MONTHS, build_duckdb_pipeline
from spark.pipeline_job import run_spark_pipeline


@task(retries=1, retry_delay_seconds=30, log_prints=True)
def stage_raw_data(dataset: str, months: list[str], refresh_duckdb: bool) -> None:
    build_duckdb_pipeline(dataset=dataset, months=months, refresh=refresh_duckdb)


@task(retries=1, retry_delay_seconds=30, log_prints=True)
def build_spark_layers() -> None:
    run_spark_pipeline()


@flow(name="nyc-taxi-medallion-flow", log_prints=True)
def nyc_taxi_medallion_flow(
    dataset: str = "yellow",
    months: list[str] = DEFAULT_MONTHS,
    refresh_duckdb: bool = False,
    run_spark: bool = True,
) -> None:
    logger = get_run_logger()
    logger.info("Starting orchestrated NYC Taxi medallion flow")
    stage_raw_data(dataset=dataset, months=months, refresh_duckdb=refresh_duckdb)
    if run_spark:
        build_spark_layers()
    logger.info("NYC Taxi medallion flow finished")


if __name__ == "__main__":
    nyc_taxi_medallion_flow()
