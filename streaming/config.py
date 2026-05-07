from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
STREAM_OUTPUT_DIR = ROOT / "data" / "streaming"
BRONZE_STREAM_DIR = STREAM_OUTPUT_DIR / "bronze"

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "nyc_taxi_bronze_ingest")

BASE_TRIP_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DEFAULT_STREAM_MONTHS = ["2025-08"]
MAX_RECORDS_PER_MESSAGE = int(os.getenv("MAX_RECORDS_PER_MESSAGE", "1000"))
