"""Executes a single requirement's check against Databricks, or synthesizes a
deterministic mock score when no credentials are configured (demo mode)."""
from __future__ import annotations

import random

from .databricks_client import get_connection, is_configured
from .loader import primary_sql, render_sql

LARGE_TABLE_BYTES_DEFAULT = 10_737_418_240


def mock_value(key: str, schema: str) -> float:
    # Seeded on (key, schema) so repeated runs of the same demo look stable, not random noise.
    rnd = random.Random(f"{key}:{schema}")
    return round(rnd.uniform(0.35, 1.0), 3)


def run_generic_check(key: str, catalog: str, schema: str) -> dict:
    sql = primary_sql(key)
    if sql is None:
        return {"status": "error", "value": None, "detail": "no check.md found for this requirement"}

    rendered, missing = render_sql(sql, catalog, schema)
    if missing:
        return {
            "status": "needs_target",
            "value": None,
            "detail": f"needs manual value(s) for: {', '.join(missing)} (per-table/column check)",
        }

    if not is_configured():
        return {"status": "ok", "value": mock_value(key, schema), "detail": "mock mode: no Databricks credentials configured"}

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(rendered)
            cols = [c[0] for c in cur.description]
            row = cur.fetchone()
            if row is None or "value" not in cols:
                return {"status": "ok", "value": None, "detail": "query returned no value column/row"}
            value = row[cols.index("value")]
            return {"status": "ok", "value": float(value) if value is not None else None, "detail": None}
    finally:
        conn.close()


def run_access_optimization(catalog: str, schema: str, large_table_bytes: int = LARGE_TABLE_BYTES_DEFAULT) -> dict:
    """Probe-mode check: DESCRIBE DETAIL per table, no single SQL query covers this."""
    if not is_configured():
        return {"status": "ok", "value": mock_value("access_optimization", schema), "detail": "mock mode: no Databricks credentials configured"}

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT table_name FROM {catalog}.information_schema.tables
                WHERE LOWER(table_schema) = LOWER('{schema}')
                  AND table_type IN ('MANAGED', 'EXTERNAL')
                  AND data_source_format = 'DELTA'
                ORDER BY table_name
                """
            )
            tables = [r[0] for r in cur.fetchall()]

            po_compacted: set[str] = set()
            try:
                cur.execute(
                    f"""
                    SELECT LOWER(table_name)
                    FROM system.storage.predictive_optimization_operations_history
                    WHERE LOWER(catalog_name) = LOWER('{catalog}') AND LOWER(schema_name) = LOWER('{schema}')
                      AND operation_type = 'COMPACTION' AND operation_status = 'SUCCESSFUL'
                      AND start_time >= current_timestamp() - INTERVAL 30 DAYS
                    GROUP BY LOWER(table_name)
                    """
                )
                po_compacted = {r[0] for r in cur.fetchall()}
            except Exception:
                pass  # system.storage schema not granted; skip this branch of the predicate

            large = 0
            optimized = 0
            for t in tables:
                cur.execute(f"DESCRIBE DETAIL {catalog}.{schema}.`{t}`")
                cols = [c[0] for c in cur.description]
                row = cur.fetchone()
                if row is None:
                    continue
                detail = dict(zip(cols, row))
                size = detail.get("sizeInBytes") or 0
                if size < large_table_bytes:
                    continue
                large += 1
                clustering = detail.get("clusteringColumns") or []
                partitions = detail.get("partitionColumns") or []
                props = detail.get("properties") or {}
                cluster_auto = str(props.get("clusterByAuto", "false")).lower() == "true"
                if clustering or partitions or cluster_auto or t.lower() in po_compacted:
                    optimized += 1

            if large == 0:
                return {"status": "ok", "value": None, "detail": "no large tables in scope"}
            return {"status": "ok", "value": optimized / large, "detail": f"{optimized}/{large} large tables have a layout strategy"}
    finally:
        conn.close()


PROBE_HANDLERS = {
    "access_optimization": run_access_optimization,
}


def run_check(key: str, catalog: str, schema: str) -> dict:
    handler = PROBE_HANDLERS.get(key)
    if handler:
        return handler(catalog, schema)
    return run_generic_check(key, catalog, schema)
