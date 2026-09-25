"""Thin wrapper around databricks-sql-connector. Two credential paths:

1. Databricks Apps runtime: DATABRICKS_HOST/DATABRICKS_CLIENT_ID/DATABRICKS_CLIENT_SECRET
   are injected automatically for the app's service principal (OAuth M2M). We bridge
   these into the SQL connector via the databricks-sdk's Config.authenticate callable,
   which is the documented pattern for using SDK credentials with the SQL connector.
2. Local development: DATABRICKS_HOST/DATABRICKS_HTTP_PATH/DATABRICKS_TOKEN (a PAT).

The SQL warehouse's HTTP path is built from DATABRICKS_WAREHOUSE_ID when running as a
Databricks App (bind a SQL warehouse resource to the app so app.yaml can populate it via
`valueFrom: sql-warehouse`); DATABRICKS_HTTP_PATH is used directly for local dev.
Without either credential set, the app runs in mock mode.
"""
from __future__ import annotations

import os


def _http_path() -> str | None:
    if os.getenv("DATABRICKS_HTTP_PATH"):
        return os.environ["DATABRICKS_HTTP_PATH"]
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    return f"/sql/1.0/warehouses/{warehouse_id}" if warehouse_id else None


def is_configured() -> bool:
    if not (os.getenv("DATABRICKS_HOST") and _http_path()):
        return False
    has_pat = bool(os.getenv("DATABRICKS_TOKEN"))
    has_m2m = bool(os.getenv("DATABRICKS_CLIENT_ID") and os.getenv("DATABRICKS_CLIENT_SECRET"))
    return has_pat or has_m2m


def get_connection():
    from databricks import sql as dbsql  # imported lazily so mock mode never needs the driver installed correctly

    http_path = _http_path()
    host = os.environ["DATABRICKS_HOST"]

    if os.getenv("DATABRICKS_TOKEN"):
        return dbsql.connect(server_hostname=host, http_path=http_path, access_token=os.environ["DATABRICKS_TOKEN"])

    from databricks.sdk.core import Config  # Databricks Apps: service-principal OAuth M2M

    cfg = Config(host=host, client_id=os.environ["DATABRICKS_CLIENT_ID"], client_secret=os.environ["DATABRICKS_CLIENT_SECRET"])
    return dbsql.connect(server_hostname=host, http_path=http_path, credentials_provider=lambda: cfg.authenticate)

