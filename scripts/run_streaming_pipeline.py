import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PREFECT_HOME = ROOT / "data" / ".prefect"
PREFECT_HOME.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("PREFECT_HOME", str(PREFECT_HOME))
os.environ.setdefault("PREFECT_MEMO_STORE_PATH", str(PREFECT_HOME / "memo_store.toml"))
os.environ.setdefault("PREFECT_SERVER_ANALYTICS_ENABLED", "false")
os.environ.setdefault("DO_NOT_TRACK", "1")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from orchestration.prefect_streaming_flow import nyc_taxi_streaming_flow
from streaming.config import DEFAULT_STREAM_MONTHS


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the streaming source-to-bronze flow")
    parser.add_argument("--dataset", default="yellow")
    parser.add_argument("--months", nargs="+", default=DEFAULT_STREAM_MONTHS)
    parser.add_argument("--chunk-size", type=int, default=5000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    nyc_taxi_streaming_flow(
        dataset=args.dataset,
        months=args.months,
        chunk_size=args.chunk_size,
    )


if __name__ == "__main__":
    main()
