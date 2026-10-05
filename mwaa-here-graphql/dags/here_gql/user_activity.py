"""userActivity MV -> Postgres (analytics_raw.airflow_user_activity).

Grain: (user, date, group).
Load is window-idempotent: inside one transaction, delete the date window and insert
the fresh rows. Re-running any window gives the same result.
"""
from __future__ import annotations

import logging
from collections import Counter
from datetime import date, timedelta

from .client import GraphQLError, GraphQLConfig, run_query
from .queries import USER_ACTIVITY_QUERY

log = logging.getLogger(__name__)

TABLE = "analytics_raw.airflow_user_activity"

# (GraphQL field, Postgres column, Postgres type). Lowercase snake_case avoids quoting issues.
COLUMNS = [
    ("id", "id", "text"),
    ("orgId", "org_id", "text"),
    ("userId", "user_id", "text"),
    ("groupId", "group_id", "text"),
    ("groupName", "group_name", "text"),
    ("date", "date", "date"),
    ("uniqueAppsUsed", "unique_apps_used", "bigint"),
    ("totalNavigations", "total_navigations", "bigint"),
    ("activeDays", "active_days", "bigint"),
    ("username", "username", "text"),
    ("givenName", "given_name", "text"),
    ("familyName", "family_name", "text"),
]
PG_COLUMNS = [c for _, c, _ in COLUMNS] + ["run_id"]

DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    {", ".join(f"{c} {t}" for _, c, t in COLUMNS)},
    run_id    text,
    loaded_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS airflow_user_activity_date_idx ON {TABLE} (date);
"""


def fetch(cfg: GraphQLConfig, start: date, end: date) -> list[dict]:
    """Pull [start, end]. Requests one extra day so it works whether the API's endDate
    is inclusive or exclusive; to_rows() trims back to the window."""
    query = USER_ACTIVITY_QUERY.format(
        start=start.isoformat(), end=(end + timedelta(days=1)).isoformat()
    )
    records = run_query(cfg, query).get("userActivity")
    if records is None:
        return []
    if not isinstance(records, list):
        raise GraphQLError(f"Expected a list for userActivity, got {type(records).__name__}")
    return records


def to_rows(records: list[dict], start: date, end: date, run_id: str | None = None) -> list[tuple]:
    """Keep only rows inside [start, end]; normalise date to YYYY-MM-DD."""
    lo, hi = start.isoformat(), end.isoformat()
    rows, outside = [], 0
    for r in records:
        d = str(r.get("date") or "")[:10]
        if not (lo <= d <= hi):
            outside += 1
            continue
        rows.append(tuple(d if f == "date" else r.get(f) for f, _, _ in COLUMNS) + (run_id,))
    if outside:
        log.info("Dropped %s rows outside %s..%s", outside, lo, hi)

    dupes = sum(n - 1 for n in Counter((r[2], r[3], r[5]) for r in rows).values() if n > 1)
    if dupes:  # (user_id, group_id, date) should be unique at this grain
        log.warning("%s duplicate (user, group, date) rows in this window", dupes)
    return rows


def rows_per_date(rows: list[tuple]) -> dict:
    return dict(sorted(Counter(r[5] for r in rows).items()))


def load_window(conn, rows: list[tuple], start: date, end: date) -> int:
    """Replace [start, end] in one transaction. Returns rows deleted. `conn` is a psycopg2 connection."""
    from psycopg2.extras import execute_values  # lazy: fetch-only local runs don't need psycopg2

    with conn:  # commit on success, rollback on any error
        with conn.cursor() as cur:
            cur.execute(DDL)
            cur.execute(f"DELETE FROM {TABLE} WHERE date BETWEEN %s AND %s", (start, end))
            deleted = cur.rowcount
            execute_values(
                cur,
                f"INSERT INTO {TABLE} ({', '.join(PG_COLUMNS)}) VALUES %s",
                rows,
                page_size=1000,
            )
    log.info("%s: replaced %s rows with %s rows for %s..%s", TABLE, deleted, len(rows), start, end)
    return deleted
