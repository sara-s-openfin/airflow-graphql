"""Connectivity check: Airflow -> here.is.here.io GraphQL over HTTPS with a self-signed JWT.

Trigger manually. To test a different query: "Trigger DAG w/ config" -> {"query": "{ ... }"}
"""
import json
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task

from mwaa_graphql.here_gql.client import config_from_connection, run_query
from mwaa_graphql.here_gql.queries import SMOKE_QUERY

log = logging.getLogger(__name__)

CONN_ID = "here_graphql"


@dag(
    dag_id="here_graphql_smoke",
    schedule=None,
    start_date=datetime(2026, 10, 1),
    catchup=False,
    params={"query": SMOKE_QUERY},
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["here", "graphql", "smoke"],
)
def here_graphql_smoke():

    @task
    def check_connectivity(params=None) -> dict:
        query = (params or {}).get("query") or SMOKE_QUERY
        cfg = config_from_connection(CONN_ID)
        log.info("Using %r", cfg)

        data = run_query(cfg, query)

        summary = {
            key: {"rows": len(val) if isinstance(val, list) else 1,
                  "sample": (val[:1] if isinstance(val, list) else val)}
            for key, val in data.items()
        }
        log.info("Response summary:\n%s", json.dumps(summary, indent=2, default=str))
        return summary  # small -> safe for XCom

    check_connectivity()


here_graphql_smoke()
