# Fix: lineage_completeness

Move writes onto paths where Unity Catalog can capture table and column lineage, and rewrite the transformations that lose column lineage.

## Context

Lineage is captured, not declared, so the fix is always on the writer side. Match the diagnostic's `status` to the cause:

- `NO_LINEAGE`: the write does not run on Unity Catalog compute, or `system.access` is not enabled or granted, or the writer ran before the window, or lag. Check grants first (cheapest), then the compute the writer uses.
- `TABLE_ONLY`: the write ran on UC compute but the engine could not derive columns. Typical culprits: `spark.createDataFrame(pandas_df)` or any local collection materialized then written, RDD-based writes, `INSERT ... VALUES`, writes from Python libraries that bypass Spark (Delta-rs, polars) against the table location, and some MERGE patterns on older runtimes. Rewriting the step as a DataFrame or SQL transformation that reads the source table directly restores column lineage.
- `PARTIAL_COLUMNS`: the untraced columns are usually literals, `current_timestamp()`, UUIDs, or outputs of Python UDFs that the planner cannot see through. Literals and generated columns legitimately have no source; document them in the column comment. For UDFs, prefer SQL expressions or `pandas_udf` over row-level Python UDFs where possible.
- `PATH_ONLY`: expected for landing tables. Nothing to fix here; the traceability of those rows is covered by `record_level_traceability` (persist `_metadata.file_path`).

None of these fixes rewrite existing data. Changing how a pipeline writes affects only future commits, and lineage for those commits appears after the system-table lag.

## Fix: Enable and grant the lineage tables

```bash
databricks system-schemas enable {{ metastore_id }} access
```

```sql
GRANT USE SCHEMA ON SCHEMA system.access TO `{{ assessment_principal }}`;
GRANT SELECT ON TABLE system.access.table_lineage  TO `{{ assessment_principal }}`;
GRANT SELECT ON TABLE system.access.column_lineage TO `{{ assessment_principal }}`;
```

Idempotent. Lineage rows are visible only for tables the caller can read, so also confirm `SELECT` on `{{ catalog }}.{{ schema }}`.

## Fix: Rewrite a local-collection write as a table-to-table transformation

A pattern that loses column lineage (the write is fed from driver memory, so the engine sees no source columns):

```python
# before: no column lineage
pdf = spark.table("{{ catalog }}.{{ schema }}.{{ source_asset }}").toPandas()
pdf["amount_usd"] = pdf["amount"] * pdf["fx_rate"]
spark.createDataFrame(pdf).write.mode("append").saveAsTable("{{ catalog }}.{{ schema }}.{{ asset }}")
```

The same step as a DataFrame transformation keeps the plan end to end and records `amount_usd <- amount, fx_rate`:

```python
from pyspark.sql import functions as F
(spark.table("{{ catalog }}.{{ schema }}.{{ source_asset }}")
      .withColumn("amount_usd", F.col("amount") * F.col("fx_rate"))
      .write.mode("append").saveAsTable("{{ catalog }}.{{ schema }}.{{ asset }}"))
```

Or in SQL, which is the most reliably traced form:

```sql
INSERT INTO {{ catalog }}.{{ schema }}.{{ asset }}
SELECT *, amount * fx_rate AS amount_usd
FROM {{ catalog }}.{{ schema }}.{{ source_asset }}
```

Both forms append; run them from the pipeline, not ad hoc, so the append is not duplicated.

## Fix: Document columns that legitimately have no source

For generated columns (`load_ts`, surrogate keys, constants), say so in the column comment so a reader of the diagnostic does not chase them. Only set the comment when it is empty (check `information_schema.columns.comment` first).

```sql
COMMENT ON COLUMN {{ catalog }}.{{ schema }}.{{ asset }}.{{ column }} IS
  'Generated at load time ({{ how }}); no upstream source column'
```

Bulk form, emitting one statement per untraced column that has no comment yet (fill the reason before running):

```sql
WITH traced AS (
    SELECT DISTINCT LOWER(target_table_name) AS table_name, LOWER(target_column_name) AS column_name
    FROM system.access.column_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND source_column_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
)
SELECT concat(
    'COMMENT ON COLUMN {{ catalog }}.{{ schema }}.`', c.table_name, '`.`', c.column_name,
    '` IS ''<fill_in: generated or literal, no upstream source column>'';'
) AS stmt
FROM {{ catalog }}.information_schema.columns c
JOIN {{ catalog }}.information_schema.tables t
  ON c.table_schema = t.table_schema AND c.table_name = t.table_name
LEFT JOIN traced tr
  ON LOWER(c.table_name) = tr.table_name AND LOWER(c.column_name) = tr.column_name
WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
  AND t.table_type IN ('MANAGED', 'EXTERNAL')
  AND tr.column_name IS NULL
  AND (c.comment IS NULL OR c.comment = '')
ORDER BY c.table_name, c.ordinal_position
```

Show the statements to the user; do not run any that still contain `<fill_in>`.

## Organizational guidance

Column lineage survives only in pipelines written as Spark SQL or DataFrame transformations from source table to target table. Make that the standard in the pipeline template (Lakeflow Declarative Pipelines enforce it by construction), review new notebooks for `toPandas()` / `collect()` before a write, and treat Python row UDFs on governed tables as needing a comment explaining the derivation. Widen the assessment window to the longest pipeline cadence so infrequent jobs are not reported as missing.
