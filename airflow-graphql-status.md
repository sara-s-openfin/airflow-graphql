# Airflow ↔ GraphQL: Status & Next Steps
_Last updated: 2026-10-05_

## Goal
1. Connect MWAA to GraphQL over HTTP for **one environment** (`here.is.here.io`), loading into Postgres.
2. Replicate that pattern across **all environments** in `analytics_pipeline`.

---

## Done

**Repo: `airflow-graphql`**
```
dags/
├── here_graphql_smoke.py           # connectivity check (orgSummary)
├── here_user_activity_to_pg.py     # userActivity → Postgres
├── .airflowignore                  # here_gql/, .ipynb_checkpoints
└── here_gql/
    ├── client.py                   # JWT signing, POST, error handling
    ├── queries.py                  # SMOKE_QUERY, USER_ACTIVITY_QUERY
    └── user_activity.py            # fetch, to_rows, load_window
scripts/
├── smoke_local.py                  # local connectivity test
└── pull_local.py                   # local fetch (+ --load)
```

**Auth (working)**
- HS256 JWT minted per request, 5-minute lifetime.
- JWT claims: `sub` (email), `preferred_username` (= `sub`), `aud` (username), `iss` (`https://here.is.here.io/`, trailing slash required), `iat`, `exp`.
- HTTP headers: `Authorization: Bearer <token>` and `x-of-auth-id`.
- Two causes of `AUTHENTICATION_FAILED`, both found and fixed:
  - "Invalid user info" means the `preferred_username` claim is missing.
  - "No authentication provider…" means the connection Extra still contains placeholder values.

**Airflow**
- Connection `here_graphql` (HTTP) was created in the UI:
  - Host is `here.is.here.io`, schema is `https`, and password is the JWT secret.
  - Extra holds `iss`, `aud`, `sub`, `auth_id` and `graphql_path`.
- Deploy path is `s3://here-airflow-data-store/dags/mwaa_graphql/`. DAG imports use the `mwaa_graphql.here_gql.*` prefix.
- **`here_graphql_smoke` ran green in MWAA.** Org `1281cbdc-ea6c-4d9e-8cb0-dedee2ba6e63` is confirmed correct.

**userActivity**
- The local fetch is verified: 176 rows for 10-02 to 10-04. The API's `endDate` is **inclusive**.
- The `here_user_activity_to_pg` DAG is built and tested against a local Postgres:
  - It writes to `analytics_raw.airflow_user_activity` and creates the table on its first run.
  - It is idempotent: each run deletes and reinserts its date window in one transaction.
  - An empty API response never wipes existing data.
  - By default it runs daily at 06:00 UTC with a 3-day trailing window. Backfill by triggering with config: `{"start_date": "...", "end_date": "..."}`.

---

## Start here (next session)

1. **Through the bastion:** run `python scripts/pull_local.py --load` and confirm that `analytics_raw.airflow_user_activity` is created and loaded.
2. In `here_user_activity_to_pg.py`, set `PG_CONN_ID` to the **existing** Postgres connection ID. Confirm that connection's type is Postgres and its database is `openfin`.
3. Decide on the schedule: keep `"0 6 * * *"`, or use `None` for manual-only runs.
4. Upload:
   ```bash
   aws s3 cp dags/ s3://here-airflow-data-store/dags/mwaa_graphql/ --recursive \
     --exclude "*__pycache__*" --exclude "*.pyc" --exclude "*.ipynb_checkpoints*"
   ```
5. Unpause and trigger `here_user_activity_to_pg`. Check the log for rows per date and the `replaced X rows with Y rows` line, then check the row counts in the table.

---

## Then: scale to multiple environments

- **Connections:** create one per environment (`here_graphql_<env>`), each with its own claims and `auth_id`.
- **Fan-out:** use dynamic task mapping, `extract_and_load.expand(env=[...])`, so each environment fails on its own, the same way `run_all.py` isolates failures today.
- **⚠ Fix the delete key before adding a second environment.** `load_window` currently deletes by **date only**, so one environment's run would wipe another's rows. Add an `env` column and delete on `(env, date)`.
- **More MVs:** turn `user_activity.py` into a config-driven MV spec (query, columns, target table) and add MVs one at a time.
- **dbt:** point staging models at the `airflow_*` tables and run `run_dbt.sh` on MWAA (via a virtualenv and `BashOperator`).

---

## Open items
- **Secrets Manager `AccessDeniedException` in logs:** harmless, because Airflow falls back to the UI connection. Waiting on DevOps to grant `GetSecretValue`.
- **Before production:** move `here_graphql` (and any per-environment connections) from the UI/metadata DB into Secrets Manager.
- **Never upload `requirements.txt`:** MWAA has one requirements file for the whole environment, and PyJWT and requests are already installed.
