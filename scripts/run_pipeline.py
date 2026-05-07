import argparse
import sys
from pathlib import Path

import duckdb
import requests

BASE_TRIP_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
DEFAULT_MONTHS = [
    "2025-01", "2025-02", "2025-03", "2025-04",
    "2025-05", "2025-06", "2025-07", "2025-08",
]
DATASET_CHOICES = ["yellow", "green", "fhv", "fhvhv"]

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
REF_DIR = ROOT / "data" / "reference"
DB_FILE = ROOT / "data" / "nyc_taxi.duckdb"
SQL_FILE = ROOT / "sql" / "elt_pipeline.sql"


def grab_file(url, where):
    where.parent.mkdir(parents=True, exist_ok=True)
    if where.exists():
        print(f"already have {where.name}")
        return
    print(f"downloading {url}")
    with requests.get(url, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        with where.open("wb") as fh:
            for block in resp.iter_content(1 << 20):
                if block:
                    fh.write(block)
    print(f"saved {where}")


def pull_trip_files(dataset, months):
    for month in months:
        name = f"{dataset}_tripdata_{month}.parquet"
        grab_file(f"{BASE_TRIP_URL}/{name}", RAW_DIR / name)


def grab_lookup():
    grab_file(ZONE_LOOKUP_URL, REF_DIR / "taxi_zone_lookup.csv")


def run_sql():
    if not SQL_FILE.exists():
        raise FileNotFoundError(f"Missing SQL: {SQL_FILE}")
    sql_text = SQL_FILE.read_text(encoding="utf-8")
    print(f"running SQL from {SQL_FILE}")
    with duckdb.connect(DB_FILE) as con:
        con.execute(sql_text)
    print("duckdb done")


def get_args():
    parser = argparse.ArgumentParser(description="quick medallion loader")
    parser.add_argument("--dataset", default="yellow", choices=DATASET_CHOICES)
    parser.add_argument("--months", nargs="+", default=DEFAULT_MONTHS)
    parser.add_argument("--refresh", action="store_true", help="wipe duckdb before loading")
    return parser.parse_args()


def build_duckdb_pipeline(dataset="yellow", months=None, refresh=False):
    if months is None:
        months = DEFAULT_MONTHS
    if refresh and DB_FILE.exists():
        print("refresh flag on -> deleting old duckdb file")
        DB_FILE.unlink()
    grab_lookup()
    pull_trip_files(dataset, months)
    run_sql()


def main():
    args = get_args()
    build_duckdb_pipeline(
        dataset=args.dataset,
        months=args.months,
        refresh=args.refresh,
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Pipeline failed: {exc}", file=sys.stderr)
        sys.exit(1)
