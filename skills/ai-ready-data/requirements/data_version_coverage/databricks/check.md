# Check: data_version_coverage

Fraction of Delta base tables whose time-travel retention covers the required reconstruction window: both `delta.logRetentionDuration` and `delta.deletedFileRetentionDuration` are at least `{{ min_retention_days }}` days.

## Context

Delta time travel (`SELECT ... VERSION AS OF` / `TIMESTAMP AS OF`, `RESTORE`) needs two things to still exist for the version you want: the transaction log entries and the data files that version referenced. Two table properties bound them:

- `delta.logRetentionDuration` (default `interval 30 days`): how long log entries are kept. Older checkpoints are removed and versions past this point are no longer addressable.
- `delta.deletedFileRetentionDuration` (default `interval 7 days`): how long `VACUUM` keeps files that are no longer part of the current version. After this, `VACUUM` deletes them and older versions become unreadable even if the log is still there.

The effective reconstruction window is the smaller of the two, so with defaults it is 7 days, not 30. That is why a table with no properties set fails at the default `{{ min_retention_days }}` of 30. The check treats the configured retention as the guarantee; a table that has never been vacuumed may happen to keep more history, but nothing prevents the next `VACUUM` from removing it.

The properties are not in `information_schema`, so this is a **probe-mode** check: enumerate tables, run `DESCRIBE DETAIL` per table, evaluate the predicate over the `properties` map, aggregate. `DESCRIBE DETAIL` needs `SELECT` on the table. The signal is native and current (no lag). Values look like `interval 30 days`, `interval 1 week` or `interval 720 hours`; the predicate normalizes hours, days and weeks. Any other spelling is treated as unparseable and the table fails, with the raw value surfaced so a human can judge.

Non-Delta tables (`data_source_format` not `DELTA`) are excluded from the population; they have no time travel to configure. Returns NULL (N/A) when no Delta base tables were probed.

## SQL

### Retention properties via DESCRIBE DETAIL (primary)

**(a) Enumerate tables in scope**

```sql
SELECT table_name
FROM {{ catalog }}.information_schema.tables
WHERE LOWER(table_schema) = LOWER('{{ schema }}')
  AND table_type IN ('MANAGED', 'EXTERNAL')
  AND UPPER(data_source_format) = 'DELTA'
ORDER BY table_name
```

**(b) Probe each table**

```sql
DESCRIBE DETAIL {{ catalog }}.{{ schema }}.`{{ asset }}`
```

The one-row result has a `properties` map. The two keys of interest are `delta.deletedFileRetentionDuration` and `delta.logRetentionDuration`; a missing key means the Delta default applies (7 days and 30 days respectively).

**(c) Per-table predicate**

In words: the deleted-file retention, in days, defaulting to 7 when unset, is at least `{{ min_retention_days }}`, and the log retention, in days, defaulting to 30 when unset, is at least `{{ min_retention_days }}`. Hours divide by 24, weeks multiply by 7.

As a SQL expression over the probe's `properties` column:

```sql
COALESCE(
    TRY_CAST(regexp_extract(LOWER(properties['delta.deletedFileRetentionDuration']),
                            'interval\\s+(\\d+)\\s+(hour|day|week)', 1) AS DOUBLE)
    * CASE regexp_extract(LOWER(properties['delta.deletedFileRetentionDuration']),
                          'interval\\s+(\\d+)\\s+(hour|day|week)', 2)
        WHEN 'hour' THEN 1.0 / 24
        WHEN 'week' THEN 7.0
        WHEN 'day'  THEN 1.0
      END,
    CASE WHEN properties['delta.deletedFileRetentionDuration'] IS NULL THEN 7.0 END
) >= {{ min_retention_days }}
AND
COALESCE(
    TRY_CAST(regexp_extract(LOWER(properties['delta.logRetentionDuration']),
                            'interval\\s+(\\d+)\\s+(hour|day|week)', 1) AS DOUBLE)
    * CASE regexp_extract(LOWER(properties['delta.logRetentionDuration']),
                          'interval\\s+(\\d+)\\s+(hour|day|week)', 2)
        WHEN 'hour' THEN 1.0 / 24
        WHEN 'week' THEN 7.0
        WHEN 'day'  THEN 1.0
      END,
    CASE WHEN properties['delta.logRetentionDuration'] IS NULL THEN 30.0 END
) >= {{ min_retention_days }}
```

The inner `COALESCE` falls back to the Delta default only when the key is absent. A key that is present but does not match the pattern yields NULL, and a NULL predicate counts as failing.

**(d) Aggregation**

```
value              = tables_passing / tables_probed
tables_passing     = count of probed tables where the predicate is TRUE
tables_probed      = count of tables from (a) whose probe succeeded
value is NULL when tables_probed = 0
```

Report `tables_passing` as the numerator column and `tables_probed` as the denominator column. Tables whose probe fails with a permission error are excluded from both and listed separately.

### Retention properties via SHOW TBLPROPERTIES (variant)

Same predicate, different probe. Useful when the runner already collects table properties for other checks. The statement returns one row per property with `key` and `value` columns; a key that is not set is simply absent from the output.

```sql
SHOW TBLPROPERTIES {{ catalog }}.{{ schema }}.`{{ asset }}`
```

Pivot the rows into a map (`map_from_entries(collect_list(struct(key, value)))`) and apply the expression from (c) with that map in place of `properties`.

### Bulk probe with the SDK (variant)

For schemas with hundreds of tables, run the probes from Python instead of one SQL round trip each. Needs a warehouse id and `SELECT` on each table.

```python
import re
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

w = WorkspaceClient()
UNIT = {"hour": 1 / 24, "day": 1.0, "week": 7.0}

def sql(stmt, wh):
    r = w.statement_execution.execute_statement(statement=stmt, warehouse_id=wh, wait_timeout="50s")
    assert r.status.state == StatementState.SUCCEEDED, r.status
    cols = [c.name for c in r.manifest.schema.columns]
    return [dict(zip(cols, row)) for row in (r.result.data_array or [])]

def days(value, default):
    if value is None:
        return default
    m = re.match(r"interval\s+(\d+)\s+(hour|day|week)", value.lower())
    return int(m.group(1)) * UNIT[m.group(2)] if m else None

def check(catalog, schema, wh, min_days=30):
    tables = sql(f"""SELECT table_name FROM {catalog}.information_schema.tables
        WHERE LOWER(table_schema) = LOWER('{schema}') AND table_type IN ('MANAGED','EXTERNAL')
          AND UPPER(data_source_format) = 'DELTA'""", wh)
    passing = probed = 0
    for t in tables:
        props = sql(f"DESCRIBE DETAIL {catalog}.{schema}.`{t['table_name']}`", wh)[0]["properties"]
        # properties arrives as a JSON-ish string from the REST API; parse it before indexing
        import json; props = json.loads(props) if isinstance(props, str) else (props or {})
        dfr = days(props.get("delta.deletedFileRetentionDuration"), 7.0)
        lr = days(props.get("delta.logRetentionDuration"), 30.0)
        probed += 1
        passing += int(dfr is not None and lr is not None and dfr >= min_days and lr >= min_days)
    return {"tables_passing": passing, "tables_probed": probed,
            "value": (passing / probed) if probed else None}
```

There is no pure-SQL approximation: neither `information_schema` nor any `system.*` table exposes Delta table properties. The diagnostic adds a SQL-only scan for explicit version columns as supporting context, but that does not measure time travel and is not a substitute for the probe.
