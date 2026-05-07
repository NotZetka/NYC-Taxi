from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from kafka import KafkaProducer
import requests

from streaming.config import (
    BASE_TRIP_URL,
    DEFAULT_STREAM_MONTHS,
    KAFKA_BROKER,
    KAFKA_TOPIC,
    MAX_RECORDS_PER_MESSAGE,
    RAW_DIR,
)


def _serialize_value(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _build_source_url(dataset: str, month: str) -> str:
    return f"{BASE_TRIP_URL}/{dataset}_tripdata_{month}.parquet"


def _download_source_file(dataset: str, month: str) -> Path:
    source_url = _build_source_url(dataset, month)
    target_path = RAW_DIR / f"{dataset}_tripdata_{month}.parquet"

    if target_path.exists() and target_path.stat().st_size > 0:
        print(f"using cached source file {target_path}")
        return target_path

    target_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"downloading source file {source_url}")
    with requests.get(source_url, stream=True, timeout=120) as response:
        response.raise_for_status()
        with target_path.open("wb") as handle:
            for block in response.iter_content(1 << 20):
                if block:
                    handle.write(block)

    print(f"saved source file {target_path}")
    return target_path


def _record_batches(records: list[dict]) -> list[list[dict]]:
    return [
        records[index : index + MAX_RECORDS_PER_MESSAGE]
        for index in range(0, len(records), MAX_RECORDS_PER_MESSAGE)
    ]


def publish_source_batches(
    dataset: str = "yellow",
    months: list[str] | None = None,
    chunk_size: int = 5000,
    run_id: str | None = None,
) -> tuple[int, int | None]:
    if months is None:
        months = DEFAULT_STREAM_MONTHS

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        compression_type="gzip",
        value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
    )
    con = duckdb.connect()
    published_batches = 0
    first_offset = None

    try:
        for month in months:
            source_url = _build_source_url(dataset, month)
            source_file = _download_source_file(dataset, month)
            total_rows = con.execute(
                "SELECT COUNT(*) FROM read_parquet(?)",
                [str(source_file)],
            ).fetchone()[0]
            print(f"publishing {total_rows} rows for {dataset} {month}")
            offset = 0

            while offset < total_rows:
                result = con.execute(
                    "SELECT * FROM read_parquet(?) LIMIT ? OFFSET ?",
                    [str(source_file), chunk_size, offset],
                )
                columns = [col[0] for col in result.description]
                rows = result.fetchall()
                records = [
                    {column: _serialize_value(value) for column, value in zip(columns, row)}
                    for row in rows
                ]

                if not records:
                    break

                for record_batch in _record_batches(records):
                    published_batches += 1
                    future = producer.send(
                        KAFKA_TOPIC,
                        partition=0,
                        value={
                            "dataset": dataset,
                            "month": month,
                            "run_id": run_id,
                            "source_url": source_url,
                            "batch_id": published_batches,
                            "offset": offset,
                            "row_count": len(record_batch),
                            "emitted_at": datetime.now(timezone.utc).isoformat(),
                            "records": record_batch,
                        },
                    )
                    metadata = future.get(timeout=60)
                    if first_offset is None:
                        first_offset = metadata.offset
                    offset += len(record_batch)
                    if published_batches % 100 == 0:
                        print(
                            f"published {published_batches} messages "
                            f"({offset}/{total_rows} rows for {dataset} {month})"
                        )

        producer.flush()
        print(f"published {published_batches} messages total")
        return published_batches, first_offset
    finally:
        producer.close()
        con.close()
