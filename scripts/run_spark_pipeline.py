import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUN_PIPELINE = ROOT / "scripts" / "run_pipeline.py"
SPARK_JOB = ROOT / "spark" / "pipeline_job.py"


def run_cmd(cmd: list[str]) -> None:
    print("$ " + " ".join(cmd))
    subprocess.run(cmd, check=True)


def orchestrate(skip_download: bool, refresh: bool) -> None:
    if not skip_download:
        download_cmd = [sys.executable, str(RUN_PIPELINE)]
        if refresh:
            download_cmd.append("--refresh")
        run_cmd(download_cmd)
    run_cmd([sys.executable, str(SPARK_JOB)])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Orchestrate DuckDB download + Spark medallion build")
    parser.add_argument("--skip-download", action="store_true", help="Do not call run_pipeline.py before Spark")
    parser.add_argument("--refresh-download", action="store_true", help="Pass --refresh to run_pipeline.py")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    orchestrate(skip_download=args.skip_download, refresh=args.refresh_download)


if __name__ == "__main__":
    main()
