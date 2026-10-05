#!/usr/bin/env python
"""Run the same client outside Airflow to prove auth + query before touching MWAA.

Usage:
  python scripts/smoke_local.py                 # runs SMOKE_QUERY
  python scripts/smoke_local.py my_query.graphql
Writes the raw JSON response to out/.
"""
import json
import logging
import pathlib
import sys
from datetime import datetime

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dags"))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

from here_gql.client import GraphQLError, config_from_env, run_query  # noqa: E402
from here_gql.queries import SMOKE_QUERY  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def main() -> int:
    query = pathlib.Path(sys.argv[1]).read_text() if len(sys.argv) > 1 else SMOKE_QUERY
    cfg = config_from_env()
    print(f"Target: {cfg.url}")
    try:
        data = run_query(cfg, query)
    except GraphQLError as e:
        print(f"FAILED: {e}")
        return 1

    out_dir = ROOT / "out"
    out_dir.mkdir(exist_ok=True)
    out_file = out_dir / f"smoke_{datetime.now():%Y%m%d_%H%M%S}.json"
    out_file.write_text(json.dumps(data, indent=2, default=str))
    print(f"OK: top-level keys {list(data)} -> {out_file}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
