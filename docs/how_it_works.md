# How It Works

## Streaming path

The new homework requirement is implemented in four steps:

1. `orchestration/prefect_streaming_flow.py`
   Prefect is the trigger layer. The flow runs two tasks:
   publish data to the queue and then consume it into bronze.

2. `streaming/source_to_queue.py`
   The producer reads source parquet files directly from the NYC TLC CDN.
   It uses DuckDB only as a lightweight reader so rows can be fetched in chunks.
   Each chunk becomes one Kafka message with metadata and raw records.

3. `streaming/bronze_consumer.py`
   The consumer listens to the Kafka topic and writes each micro-batch to a JSONL file.
   That output is the new streaming bronze layer.

4. `docker-compose.yml`
   Redpanda provides the external queue system locally.

## Why this matches the assignment

- There is an explicit queue while data is moving.
- Source data is injected automatically by an orchestrated trigger, not manually copied.
- The architecture now shows both the queue and the trigger path.

## Existing batch path

The older DuckDB and Spark jobs are still present because they support the downstream medallion layers:

- DuckDB handles reproducible local ingestion and SQL modeling.
- Spark remains the scalable batch engine for silver and gold.
