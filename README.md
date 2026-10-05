# airflow-graphql 

mwaa-here-graphql

Single-environment connectivity: MWAA -> https://here.is.here.io GraphQL over HTTPS, using a self-signed HS256 JWT.
Scope: prove the connection and get JSON back. Postgres loading comes later.

```
.env / Secrets Manager (airflow/connections/here_graphql)
        │  host, JWT secret, iss/aud/sub/auth_id
        ▼
dags/here_gql/client.py
  build_token()   # HS256, exp = 5 min, minted per request
  run_query()     # POST + Authorization: Bearer + x-of-auth-id
        │
        ├── scripts/smoke_local.py      # laptop: writes JSON to out/
        └── dags/here_graphql_smoke.py  # MWAA: logs summary, returns to XCom
```

## Steps

1. **Local test (no Airflow):**
   ```bash
   cp .env.example .env            # fill in values
   pip install -r requirements-local.txt
   python scripts/smoke_local.py   # -> OK + out/smoke_<ts>.json
   ```
   Don't move on until this passes.

2. **Create the connection secret:**
   ```bash
   cp secrets/here_graphql.connection.example.json secrets/here_graphql.connection.json   # fill in
   AWS_REGION=<region> ./scripts/create_secret.sh
   ```

3. **Turn on the Secrets Manager backend.** Copy the options in `infra/mwaa-airflow-config.txt` into MWAA -> Edit -> Airflow configuration options.

4. **Grant the MWAA execution role access to the secret** using `infra/mwaa-secrets-policy.json`. Fill in the region and account ID.

5. **Check networking (DevOps).** MWAA workers need outbound HTTPS (443) to `here.is.here.io`. That means NAT egress, or a VPC route if the host is internal-only.

6. **Deploy:** upload `dags/` (including `here_gql/` and `.airflowignore`) and `requirements.txt` to the MWAA S3 bucket, then update the environment.

7. **Run it:** trigger `here_graphql_smoke` and check the task log for `HTTP 200` and the response summary.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `Connection 'here_graphql' not found` | Secrets backend not configured, wrong prefix, or IAM missing |
| `Connection ... is missing: ...` | Secret JSON missing a field or wrongly nested |
| `HTTP 401/403` | Secret or claims differ from `extract.auth.build_token`, or wrong `x-of-auth-id` |
| `GraphQL errors: ...` | Query/field names. Auth and network are fine at this point |
| Timeout / connection error | MWAA networking (step 5) |

## Notes
- The token is never stored or logged. `GraphQLConfig.__repr__` hides the secret.
- This repo deliberately does not use `HttpHook`. `HttpHook` sends connection Extra keys as HTTP headers, which would send the claims as junk headers.
- The JWT claims are `iss`, `aud`, `sub`, `iat`, `exp`. If your existing `build_token` adds anything else, add it in `client.build_token`.
