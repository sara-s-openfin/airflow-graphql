#!/usr/bin/env python
"""Run the userActivity extract (and optionally the load) from your laptop.

  python scripts/pull_local.py                                # last 3 days, fetch only
  python scripts/pull_local.py --start 2026-10-01 --end 2026-10-04
  python scripts/pull_local.py --load                         # also load to Postgres

--load reads standard libpq env vars from .env:
  PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD, PGSSLMODE (optional)
"""
import argparse
import json
import logging
import pathlib
import sys
from datetime import date, datetime, timedelta

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dags"))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

from here_gql.client import config_from_env  # noqa: E402
from here_gql.user_activity import TABLE, fetch, load_window, rows_per_date, to_rows  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def main() -> int:
    yesterday = date.today() - timedelta(days=1)
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=date.fromisoformat, default=yesterday - timedelta(days=2))
    ap.add_argument("--end", type=date.fromisoformat, default=yesterday)
    ap.add_argument("--load", action="store_true", help=f"load into {TABLE}")
    a = ap.parse_args()

    records = fetch(config_from_env(), a.start, a.end)
    rows = to_rows(records, a.start, a.end, run_id="local")
    print(f"Window {a.start}..{a.end}: fetched {len(records)}, in window {len(rows)}")
    print("Per date:", json.dumps(rows_per_date(rows), indent=2))
    if records:
        print("Dates returned by API:", sorted({str(r.get('date'))[:10] for r in records}))

    out = ROOT / "out" / f"user_activity_{a.start}_{a.end}_{datetime.now():%H%M%S}.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(records, indent=2, default=str))
    print(f"Raw JSON -> {out}")

    if a.load:
        if not rows:
            print("0 rows in window; not loading.")
            return 0
        import psycopg2
        conn = psycopg2.connect("")  # libpq env vars
        try:
            deleted = load_window(conn, rows, a.start, a.end)
        finally:
            conn.close()
        print(f"Loaded {len(rows)} rows into {TABLE} (replaced {deleted})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
