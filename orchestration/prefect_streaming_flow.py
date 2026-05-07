from __future__ import annotations

from uuid import uuid4

from prefect import flow, get_run_logger, task

from streaming.config import DEFAULT_STREAM_MONTHS
from streaming.bronze_consumer import consume_bronze_batches
from streaming.source_to_queue import publish_source_batches


@task(retries=2, retry_delay_seconds=15, log_prints=True)
def publish_batches_task(dataset: str, months: list[str], chunk_size: int, run_id: str) -> tuple[int, int | None]:
    return publish_source_batches(
        dataset=dataset,
        months=months,
        chunk_size=chunk_size,
        run_id=run_id,
    )


@task(retries=2, retry_delay_seconds=15, log_prints=True)
def consume_batches_task(expected_batches: int, run_id: str, start_offset: int | None) -> int:
    return consume_bronze_batches(
        max_batches=expected_batches,
        consumer_group=f"nyc-taxi-bronze-consumer-{run_id}",
        run_id=run_id,
        start_offset=start_offset,
    )


@flow(name="nyc-taxi-streaming-flow", log_prints=True)
def nyc_taxi_streaming_flow(
    dataset: str = "yellow",
    months: list[str] = DEFAULT_STREAM_MONTHS,
    chunk_size: int = 5000,
) -> None:
    logger = get_run_logger()
    run_id = uuid4().hex
    logger.info("Starting source-to-bronze streaming flow")
    published_batches, first_offset = publish_batches_task(
        dataset=dataset,
        months=months,
        chunk_size=chunk_size,
        run_id=run_id,
    )
    consumed_batches = consume_batches_task(
        expected_batches=published_batches,
        run_id=run_id,
        start_offset=first_offset,
    )
    if consumed_batches != published_batches:
        raise RuntimeError(
            f"Streaming flow consumed {consumed_batches} of {published_batches} published batches"
        )
    logger.info(
        "Streaming flow finished with %s published and %s consumed batches",
        published_batches,
        consumed_batches,
    )


if __name__ == "__main__":
    nyc_taxi_streaming_flow()
