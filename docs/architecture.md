# NYC Taxi Architecture

```mermaid
flowchart LR
    classDef source fill:#f5f1e8,stroke:#8d6e63,color:#2b2118
    classDef orchestration fill:#e3f2fd,stroke:#1e88e5,color:#0d223a
    classDef queue fill:#ede7f6,stroke:#5e35b1,color:#231942
    classDef processing fill:#e8f5e9,stroke:#43a047,color:#17311c
    classDef storage fill:#fff8e1,stroke:#ffb300,color:#3f2d00
    classDef consumer fill:#fce4ec,stroke:#d81b60,color:#3c1021

    TLC["NYC TLC source files"]:::source
    PREFECT["Prefect trigger / scheduled flow"]:::orchestration
    PRODUCER["Streaming producer\nsource_to_queue.py"]:::processing
    KAFKA["Redpanda / Kafka topic\nnyc_taxi_bronze_ingest"]:::queue
    CONSUMER["Bronze consumer\nbronze_consumer.py"]:::processing
    STREAM_BRONZE["data/streaming/bronze"]:::storage
    DUCK["DuckDB batch ingestion"]:::processing
    SPARK["PySpark batch transforms"]:::processing
    RAW["data/raw + data/reference"]:::storage
    DUCKDB["data/nyc_taxi.duckdb"]:::storage
    GOLD["data/spark/gold"]:::storage
    BI["BI / notebooks / reviewer"]:::consumer

    TLC --> PREFECT
    PREFECT --> PRODUCER
    PRODUCER --> KAFKA
    KAFKA --> CONSUMER
    CONSUMER --> STREAM_BRONZE

    TLC --> DUCK
    DUCK --> RAW
    DUCK --> DUCKDB
    RAW --> SPARK
    SPARK --> GOLD

    STREAM_BRONZE --> BI
    DUCKDB --> BI
    GOLD --> BI
```

## Streaming addition

- The new piece is the queue between source and bronze: the source is read automatically by a Prefect-triggered producer and pushed to Kafka-compatible Redpanda.
- The consumer drains queue messages into raw bronze batch files under `data/streaming/bronze`.
- The previous DuckDB and Spark layers remain in place, so the project now contains both batch and streaming patterns.
