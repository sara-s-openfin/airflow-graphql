"""userActivity MV (here.is.here.io GraphQL) -> analytics_raw.airflow_user_activity.

Daily at 06:00 UTC. Each run reloads a trailing window (default 3 days) so late MV
refreshes are picked up. Re-running is safe: the window is replaced, never appended.

Backfill / ad-hoc: "Trigger DAG w/ config" ->
  {"start_date": "2026-09-01", "end_date": "2026-09-30"}
"""
import json
import logging
from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from mwaa_graphql.here_gql.client import config_from_connection
from mwaa_graphql.here_gql.user_activity import TABLE, fetch, load_window, rows_per_date, to_rows

log = logging.getLogger(__name__)

GQL_CONN_ID = "here_graphql"
PG_CONN_ID = "license-data" # airflow connection to database


@dag(
    dag_id="here_user_activity_to_pg",
    schedule="0 6 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    params={"start_date": "", "end_date": "", "lookback_days": 3},
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["here", "graphql", "postgres"],
)
def here_user_activity_to_pg():

    @task
    def extract_and_load(params=None, ds=None, run_id=None) -> dict:
        p = params or {}
        end = date.fromisoformat(p.get("end_date") or ds)
        if p.get("start_date"):
            start = date.fromisoformat(p["start_date"])
        else:
            start = end - timedelta(days=int(p.get("lookback_days") or 3) - 1)
        if start > end:
            raise ValueError(f"start_date {start} is after end_date {end}")
        log.info("Window: %s..%s", start, end)

        records = fetch(config_from_connection(GQL_CONN_ID), start, end)
        rows = to_rows(records, start, end, run_id)
        log.info("Fetched %s records, %s in window. Per date:\n%s",
                 len(records), len(rows), json.dumps(rows_per_date(rows), indent=2))

        if not rows:
            # Never wipe a window on an empty response; it may be an API or refresh issue.
            log.warning("0 rows for %s..%s; skipping load so existing data is kept.", start, end)
            return {"start": str(start), "end": str(end), "loaded": 0, "skipped": True}

        conn = PostgresHook(postgres_conn_id=PG_CONN_ID).get_conn()
        try:
            deleted = load_window(conn, rows, start, end)
        finally:
            conn.close()

        return {"table": TABLE, "start": str(start), "end": str(end),
                "loaded": len(rows), "replaced": deleted}

    extract_and_load()


here_user_activity_to_pg()
