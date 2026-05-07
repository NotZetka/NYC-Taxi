# NYC Taxi Medallion Pipeline

This repo now contains three stages of the same homework:

- DuckDB batch medallion pipeline
- Prefect-orchestrated batch pipeline
- Streaming source-to-bronze pipeline with a queue

## New streaming homework scope

The latest task asked for:

- a queue system while data is moving,
- automatic source read and injection triggered by orchestration,
- updated architecture.

This version implements that with:

- `Prefect` as the trigger/orchestrator,
- `Redpanda` (Kafka-compatible broker) as the external queue,
- a producer that reads TLC source data in chunks,
- a consumer that writes raw bronze files to `data/streaming/bronze`.

## Main files

- `orchestration/prefect_streaming_flow.py` - Prefect flow for source-to-bronze streaming.
- `streaming/source_to_queue.py` - producer that reads source parquet and publishes micro-batches to Kafka.
- `streaming/bronze_consumer.py` - consumer that drains the queue into bronze JSONL files.
- `scripts/run_streaming_pipeline.py` - local launcher for the streaming flow.
- `docker-compose.yml` - local Redpanda broker.
- `docs/architecture.md` - updated architecture diagram.

The previous batch code is still available:

- `scripts/run_pipeline.py`
- `orchestration/prefect_flow.py`
- `spark/pipeline_job.py`
- `sql/elt_pipeline.sql`

## Local run for streaming

Start the broker:

```bash
docker compose up -d
```

Install Python dependencies:

```bash
python -m venv .venv
. .venv/Scripts/activate
pip install -r requirements.txt
```

Run the streaming flow:

```bash
python scripts/run_streaming_pipeline.py --months 2025-08 --chunk-size 5000
```

What happens:

1. Prefect starts the streaming flow.
2. The producer downloads the TLC parquet into `data/raw` if needed, then reads it locally.
3. Rows are chunked into micro-batches and pushed to the Kafka topic `nyc_taxi_bronze_ingest`.
4. The bronze consumer reads those messages and writes raw JSONL files into `data/streaming/bronze/<dataset>/<month>/`.

## Batch path still available

If you want to show the older batch part too:

```bash
python scripts/run_pipeline.py --refresh
```

## Notes

- Raw, Spark, and streaming outputs are ignored in git.
- Spark still depends on a compatible local Java installation.
- Streaming mode is focused on the `source -> queue -> bronze` requirement from the latest homework.
