from __future__ import annotations

import json
from pathlib import Path

from kafka import KafkaConsumer, TopicPartition

from streaming.config import BRONZE_STREAM_DIR, KAFKA_BROKER, KAFKA_TOPIC


def _write_batch_file(target_path: Path, records: list[dict]) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with target_path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record))
            handle.write("\n")


def consume_bronze_batches(
    max_batches: int | None = None,
    consumer_group: str = "nyc-taxi-bronze-consumer",
    run_id: str | None = None,
    start_offset: int | None = None,
) -> int:
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BROKER,
        enable_auto_commit=False,
        consumer_timeout_ms=60000,
        fetch_max_bytes=104857600,
        max_partition_fetch_bytes=104857600,
        group_id=consumer_group,
        value_deserializer=lambda payload: json.loads(payload.decode("utf-8")),
    )
    processed = 0

    try:
        partition = TopicPartition(KAFKA_TOPIC, 0)
        consumer.assign([partition])
        if start_offset is None:
            consumer.seek_to_beginning(partition)
        else:
            consumer.seek(partition, start_offset)

        for message in consumer:
            payload = message.value
            if run_id is not None and payload.get("run_id") != run_id:
                continue

            dataset = payload["dataset"]
            month = payload["month"]
            batch_id = payload["batch_id"]
            records = payload["records"]
            target_path = BRONZE_STREAM_DIR / dataset / month / f"batch_{batch_id:05d}.jsonl"

            _write_batch_file(target_path, records)
            consumer.commit()
            processed += 1
            if processed % 100 == 0:
                print(f"consumed {processed} messages")

            if max_batches is not None and processed >= max_batches:
                break
        print(f"consumed {processed} messages total")
        return processed
    finally:
        consumer.close()
