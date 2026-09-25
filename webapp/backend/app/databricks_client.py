"""Thin wrapper around databricks-sql-connector. Credentials come from the
environment only (never from the browser/client) — DATABRICKS_HOST,
DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN. Without them the app runs in mock mode."""
from __future__ import annotations

import os


def is_configured() -> bool:
    return bool(
        os.getenv("DATABRICKS_HOST") and os.getenv("DATABRICKS_HTTP_PATH") and os.getenv("DATABRICKS_TOKEN")
    )


def get_connection():
    from databricks import sql as dbsql  # imported lazily so mock mode never needs the driver installed correctly

    return dbsql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
