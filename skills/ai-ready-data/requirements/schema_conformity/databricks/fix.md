# Fix: schema_conformity

Promote rescued fields into the schema, convert permissive columns to their intended type, and make ingestion evolve the schema on purpose instead of by rescue.

## Context

Two different defects share this requirement:

**Rescued data.** The producer sends fields or types the declared schema does not hold. Fixes, in order: (1) if the new field is wanted, add it to the schema and backfill it from `_rescued_data`; (2) if a declared column's type is too narrow for what arrives, widen it; (3) if the field is noise, leave it rescued and stop counting it, or drop it at ingest with `cloudFiles.schemaEvolutionMode = 'none'` and a `select`. Auto Loader with `addNewColumns` (the default) does step 1 automatically for new top-level columns on the next run, but not for type conflicts.

**Permissive types.** A column declared STRING (or DOUBLE for a count) that should be a stricter type. Databricks can change a Delta column's type in place only for widening (`INT` to `BIGINT`, `FLOAT` to `DOUBLE`, `DECIMAL` scale increase, `DATE` to `TIMESTAMP_NTZ`, and integer types to `DECIMAL`/`DOUBLE`), and only with type widening enabled (`delta.enableTypeWidening = true`, DBR 15.4+). STRING to anything, and narrowing, are not in-place operations. The safe pattern is: add a typed column, backfill it with `TRY_CAST` after normalizing the known bad shapes, verify zero loss, then switch consumers and retire the old column. Renaming or dropping the old column requires column mapping (`delta.columnMapping.mode = 'name'`), which is a protocol upgrade; do it only when the table's readers support it.

Never `CREATE OR REPLACE TABLE` to change a type: it destroys history and breaks streams and Delta Sync indexes.

## Fix: Blast radius

```sql
SELECT
    COUNT(*)                                                                    AS total_rows,
    COUNT_IF({{ rescue_column }} IS NOT NULL)                                   AS rescued_rows,
    COUNT_IF({{ column }} IS NOT NULL AND TRY_CAST({{ column }} AS {{ target_type }}) IS NULL) AS uncastable_rows,
    COUNT_IF({{ column }} IS NOT NULL
             AND TRY_CAST({{ column }} AS {{ target_type }}) IS NULL
             AND TRY_CAST({{ normalized_expression }} AS {{ target_type }}) IS NOT NULL) AS castable_after_normalization
FROM {{ catalog }}.{{ schema }}.{{ asset }}
```

`{{ normalized_expression }}` is the cleanup for the causes the diagnostic found, for example `regexp_replace({{ column }}, '[,$ ]', '')` for numbers or `COALESCE(TRY_TO_TIMESTAMP({{ column }}), TRY_TO_TIMESTAMP({{ column }}, 'dd/MM/yyyy'))` for dates. Default: `{{ column }}` (no normalization).

## Fix: Promote a rescued field into the schema

Adds the column and backfills it from the rescue JSON. `ADD COLUMNS` has no `IF NOT EXISTS` clause and fails on a duplicate name, so the `information_schema` guard runs first.

```sql
-- Guard: skip if the column already exists
SELECT 1
FROM {{ catalog }}.information_schema.columns
WHERE LOWER(table_schema) = LOWER('{{ schema }}')
  AND LOWER(table_name)   = LOWER('{{ asset }}')
  AND LOWER(column_name)  = LOWER('{{ new_column }}');

ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ADD COLUMNS ({{ new_column }} {{ target_type }} COMMENT 'Promoted from {{ rescue_column }} on {{ today }}');

UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET {{ new_column }} = TRY_CAST(get_json_object({{ rescue_column }}, '$.{{ rescued_field }}') AS {{ target_type }}),
    {{ rescue_column }} = CASE
        WHEN size(map_keys(from_json({{ rescue_column }}, 'MAP<STRING, STRING>'))) <= 2
             AND get_json_object({{ rescue_column }}, '$.{{ rescued_field }}') IS NOT NULL THEN NULL
        ELSE to_json(map_filter(from_json({{ rescue_column }}, 'MAP<STRING, STRING>'), (k, v) -> k <> '{{ rescued_field }}'))
    END
WHERE {{ new_column }} IS NULL
  AND get_json_object({{ rescue_column }}, '$.{{ rescued_field }}') IS NOT NULL;
```

The second assignment removes the promoted field from `_rescued_data` and clears it entirely when only that field and `_file_path` remained, so the rescue-only check moves. Auto Loader adds the column to its inferred schema on the next run once it exists in the table.

## Fix: Add a typed column and backfill (STRING to a stricter type)

```sql
-- 1. Add the typed column (guard as above)
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ADD COLUMNS ({{ column }}_typed {{ target_type }} COMMENT 'Typed replacement for {{ column }}; see schema_conformity fix');

-- 2. Backfill with normalization; only rows not yet converted
UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET {{ column }}_typed = TRY_CAST({{ normalized_expression }} AS {{ target_type }})
WHERE {{ column }}_typed IS NULL
  AND {{ column }} IS NOT NULL;

-- 3. Verify: every non-null source value converted
SELECT
    COUNT_IF({{ column }} IS NOT NULL AND {{ column }}_typed IS NULL) AS lost_values,
    COUNT_IF({{ column }} IS NOT NULL)                                AS source_values
FROM {{ catalog }}.{{ schema }}.{{ asset }};
```

Proceed only when `lost_values` is zero, or when the remaining values have been reviewed and are genuinely not of the target type (then NULL is the right answer and the review should be recorded in the column comment). Then point consumers (views, features, pipelines) at `{{ column }}_typed`. To take over the original name:

```sql
-- Requires column mapping; protocol upgrade, check readers first
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5');

ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }} RENAME COLUMN {{ column }} TO {{ column }}_legacy;
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }} RENAME COLUMN {{ column }}_typed TO {{ column }};
```

Keep `{{ column }}_legacy` until the next assessment confirms nothing reads it; dropping is a separate decision (`ALTER TABLE ... DROP COLUMN` is metadata-only with column mapping, and the data stays until `VACUUM`).

## Fix: Widen a type in place

For the widenings Delta supports (`TINYINT`/`SMALLINT`/`INT` to `BIGINT`, `FLOAT` to `DOUBLE`, `DECIMAL(p,s)` to larger `p` with same or larger `s`, `DATE` to `TIMESTAMP_NTZ`, integers to `DECIMAL` or `DOUBLE`). DBR 15.4+.

```sql
-- Guard: SHOW TBLPROPERTIES {{ catalog }}.{{ schema }}.{{ asset }}  ->  delta.enableTypeWidening
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true');

ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ALTER COLUMN {{ column }} TYPE {{ target_type }};
```

Type widening is a Delta table feature; readers older than DBR 15.4 (and some external engines) cannot read the table afterwards. Check `DESCRIBE DETAIL` for `tableFeatures` and the consumer list before enabling it.

## Fix: Bulk generation of typed-column backfills

Emits the add-and-backfill pair for every STRING column the discovery variant proposed a type for. Review the type proposals first; the statements are only as good as the names.

```sql
WITH proposed AS (
    SELECT
        c.column_name,
        CASE
            WHEN REGEXP_LIKE(LOWER(c.column_name), '(uuid|guid)')                          THEN NULL
            WHEN REGEXP_LIKE(LOWER(c.column_name), '(^is_|^has_|_flag$)')                  THEN 'BOOLEAN'
            WHEN REGEXP_LIKE(LOWER(c.column_name), '(_at$|_ts$|_time$|_timestamp$)')       THEN 'TIMESTAMP'
            WHEN REGEXP_LIKE(LOWER(c.column_name), '(_date$|^date_|_dt$)')                 THEN 'DATE'
            WHEN REGEXP_LIKE(LOWER(c.column_name), '(_amount$|_amt$|_price$|_total$|_cost$|_revenue$)') THEN 'DECIMAL(18,4)'
            WHEN REGEXP_LIKE(LOWER(c.column_name), '(_count$|_cnt$|_qty$|_quantity$)')     THEN 'BIGINT'
        END AS target_type
    FROM {{ catalog }}.information_schema.columns c
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND LOWER(c.table_name)   = LOWER('{{ asset }}')
      AND c.data_type = 'STRING'
),
already AS (
    SELECT LOWER(column_name) AS column_name
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND LOWER(table_name)   = LOWER('{{ asset }}')
)
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`{{ asset }}` ADD COLUMNS (`', p.column_name, '_typed` ', p.target_type, '); ',
    'UPDATE {{ catalog }}.{{ schema }}.`{{ asset }}` SET `', p.column_name, '_typed` = TRY_CAST(`', p.column_name, '` AS ', p.target_type,
    ') WHERE `', p.column_name, '_typed` IS NULL AND `', p.column_name, '` IS NOT NULL;'
) AS stmt
FROM proposed p
LEFT JOIN already a ON a.column_name = concat(LOWER(p.column_name), '_typed')
WHERE p.target_type IS NOT NULL
  AND a.column_name IS NULL
ORDER BY p.column_name
```

## Organizational guidance

Schema conformity is decided at ingest. Declare schemas explicitly for silver tables (`schemaHints` in Auto Loader, or a full DDL) and let bronze rescue freely; then treat a non-empty `_rescued_data` in bronze as a signal that the contract changed, with an alert on `COUNT_IF(_rescued_data IS NOT NULL)` in the pipeline event log rather than a fix in the table. Pick a schema evolution mode per pipeline on purpose (`addNewColumns` for internal producers you trust, `rescue` for external feeds, `failOnNewColumns` for regulated gold tables). Store business types as business types from the start: STRING is a landing type, not a storage type, and every column that leaves bronze should have the type its consumers cast to.
