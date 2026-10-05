"""Minimal GraphQL-over-HTTPS client with self-signed HS256 JWT auth.

Works with or without Airflow:
  - config_from_env()         -> local testing (.env)
  - config_from_connection()  -> inside Airflow / MWAA (HTTP connection)
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from urllib.parse import urlsplit

import jwt
import requests

log = logging.getLogger(__name__)

DEFAULT_PATH = "/analytics/api/graphql"


class GraphQLError(RuntimeError):
    """Raised for auth failures, non-JSON responses, or a GraphQL `errors` payload."""


@dataclass(frozen=True, repr=False)
class GraphQLConfig:
    base_url: str          # https://here.is.here.io
    path: str              # /analytics/api/graphql
    jwt_secret: str
    iss: str
    aud: str
    sub: str
    auth_id: str           # sent as x-of-auth-id (org context)
    token_ttl: int = 300   # seconds; a fresh token is minted per request
    timeout: int = 60

    @property
    def url(self) -> str:
        return self.base_url.rstrip("/") + "/" + self.path.lstrip("/")

    def __repr__(self) -> str:  # keep the secret out of logs and tracebacks
        return f"GraphQLConfig(url={self.url!r}, iss={self.iss!r}, aud={self.aud!r}, sub={self.sub!r})"


# ---------- auth ----------

def build_token(cfg: GraphQLConfig) -> str:
    """Sign a short-lived HS256 JWT. Mirror extract.auth.build_token claims exactly."""
    now = int(time.time())
    claims = {
        "sub": cfg.sub,
        "preferred_username": cfg.sub,   # server resolves the user from this
        "aud": cfg.aud,
        "iss": cfg.iss,
        "iat": now,
        "exp": now + cfg.token_ttl,
    }
    return jwt.encode(claims, cfg.jwt_secret, algorithm="HS256")


def build_headers(cfg: GraphQLConfig) -> dict:
    return {
        "Authorization": f"Bearer {build_token(cfg)}",
        "x-of-auth-id": cfg.auth_id,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


# ---------- request ----------

def run_query(cfg: GraphQLConfig, query: str, variables: dict | None = None) -> dict:
    """POST a query and return `data`. Raises GraphQLError on any failure."""
    payload = {"query": query}
    if variables:
        payload["variables"] = variables

    resp = requests.post(cfg.url, json=payload, headers=build_headers(cfg), timeout=cfg.timeout)
    log.info("POST %s -> HTTP %s (%.2fs)", cfg.url, resp.status_code, resp.elapsed.total_seconds())

    if resp.status_code in (401, 403):
        raise GraphQLError(
            f"HTTP {resp.status_code}: auth rejected. Check JWT secret, iss/aud/sub, "
            f"and x-of-auth-id. Body: {resp.text[:300]}"
        )
    if resp.status_code >= 400:
        raise GraphQLError(f"HTTP {resp.status_code}: {resp.text[:1000]}")

    try:
        body = resp.json()
    except ValueError as e:
        raise GraphQLError(f"Non-JSON response: {resp.text[:300]}") from e

    if body.get("errors"):  # GraphQL errors usually arrive with HTTP 200
        raise GraphQLError(f"GraphQL errors: {body['errors']}")
    return body.get("data") or {}


# ---------- config sources ----------

def config_from_env(prefix: str = "HERE_") -> GraphQLConfig:
    """Local testing. Reads HERE_GRAPHQL_URL, HERE_JWT_SECRET, HERE_ISS, HERE_AUD, HERE_SUB, HERE_AUTH_ID."""
    keys = ["GRAPHQL_URL", "JWT_SECRET", "ISS", "AUD", "SUB", "AUTH_ID"]
    vals = {k: os.environ.get(prefix + k, "").strip() for k in keys}
    missing = [prefix + k for k, v in vals.items() if not v]
    if missing:
        raise ValueError(f"Missing env vars: {', '.join(missing)}")

    parts = urlsplit(vals["GRAPHQL_URL"])
    path = parts.path if parts.path not in ("", "/") else DEFAULT_PATH
    return GraphQLConfig(
        base_url=f"{parts.scheme}://{parts.netloc}",
        path=path,
        jwt_secret=vals["JWT_SECRET"],
        iss=vals["ISS"],
        aud=vals["AUD"],
        sub=vals["SUB"],
        auth_id=vals["AUTH_ID"],
    )


def config_from_connection(conn_id: str = "here_graphql") -> GraphQLConfig:
    """Airflow/MWAA. Reads an HTTP connection (host, schema, password=JWT secret, extra=claims)."""
    from airflow.hooks.base import BaseHook  # lazy: keeps local testing Airflow-free

    c = BaseHook.get_connection(conn_id)
    x = c.extra_dejson
    host = (c.host or "").strip()

    missing = [k for k in ("iss", "aud", "sub", "auth_id") if not x.get(k)]
    if not host:
        missing.append("host")
    if not c.password:
        missing.append("password (JWT secret)")
    if missing:
        raise ValueError(f"Connection '{conn_id}' is missing: {', '.join(missing)}")

    base = host if "://" in host else f"{c.schema or 'https'}://{host}"
    if c.port:
        base = f"{base}:{c.port}"

    return GraphQLConfig(
        base_url=base,
        path=x.get("graphql_path", DEFAULT_PATH),
        jwt_secret=c.password,
        iss=x["iss"],
        aud=x["aud"],
        sub=x["sub"],
        auth_id=x["auth_id"],
        token_ttl=int(x.get("token_ttl", 300)),
        timeout=int(x.get("timeout", 60)),
    )
