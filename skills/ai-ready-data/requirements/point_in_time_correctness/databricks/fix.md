# Fix: point_in_time_correctness

Declare a time-series primary key on feature tables so point-in-time joins are possible.

## Context

The fix is a constraint change, not a data change, but it has preconditions:

- The timestamp column must exist, be `TIMESTAMP`, `TIMESTAMP_NTZ` or `DATE`, and be `NOT NULL`. Adding `NOT NULL` fails if any row is null, so check first.
- A table can carry only one primary key. If one exists without the flag, it has to be dropped and re-added with the flag. `ALTER TABLE ... DROP PRIMARY KEY` removes only the constraint metadata; no data is touched and no version is rewritten. Foreign keys in other tables that reference this key must be dropped first (`DROP PRIMARY KEY CASCADE` does that in one step); the guard below finds them so the operator can decide.
- Primary keys in Unity Catalog are informational (not enforced). Feature Engineering relies on them anyway, so the key must actually be unique per `(entity, timestamp)`; the `uniqueness` requirement measures that. A time-series key with duplicate rows produces nondeterministic point-in-time lookups.

Only one column can be `TIMESERIES`. Choose the event time (when the feature value became true), not the load time.

Requires ownership of the table or `MANAGE` on it.

## Fix: Add a time-series primary key to a table that has none

Guard for the constraint:

```sql
SELECT constraint_name
FROM {{ catalog }}.information_schema.table_constraints
WHERE LOWER(table_schema) = LOWER('{{ schema }}')
  AND LOWER(table_name)  = LOWER('{{ asset }}')
  AND constraint_type = 'PRIMARY KEY'
```

Skip to the next section if a row exists. Blast-radius check for nullability (a primary key column cannot be nullable):

```sql
SELECT COUNT_IF({{ key_column }} IS NULL)       AS null_keys,
       COUNT_IF({{ timestamp_column }} IS NULL) AS null_timestamps,
       COUNT(*)                                 AS total_rows
FROM {{ catalog }}.{{ schema }}.{{ asset }}
```

Both null counts must be zero. Then:

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }} ALTER COLUMN {{ key_column }} SET NOT NULL;
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }} ALTER COLUMN {{ timestamp_column }} SET NOT NULL;

ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ADD CONSTRAINT {{ asset }}_pk PRIMARY KEY ({{ key_column }}, {{ timestamp_column }} TIMESERIES);
```

`SET NOT NULL` is a no-op when the column already is; check `information_schema.columns.is_nullable` if you want to skip it.

## Fix: Replace an existing primary key with a time-series one

Find dependents first. Foreign keys that reference this key block the drop:

```sql
SELECT rc.constraint_name AS fk_name, kcu.table_schema, kcu.table_name
FROM {{ catalog }}.information_schema.referential_constraints rc
JOIN {{ catalog }}.information_schema.key_column_usage kcu
  ON kcu.constraint_name = rc.constraint_name
WHERE LOWER(rc.unique_constraint_schema) = LOWER('{{ schema }}')
  AND rc.unique_constraint_name = '{{ existing_pk_name }}'
```

If rows come back, the referencing tables lose their foreign key when the primary key is dropped; re-add them after the new key exists (`relationship_declaration` will otherwise regress). Then, after the nullability check from the previous section:

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }} DROP PRIMARY KEY CASCADE;

ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }} ALTER COLUMN {{ timestamp_column }} SET NOT NULL;

ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ADD CONSTRAINT {{ asset }}_pk PRIMARY KEY ({{ key_column }}, {{ timestamp_column }} TIMESERIES);
```

Run the three statements together and confirm with `SHOW CREATE TABLE`.

## Fix: Generate ALTER statements for feature tables missing the flag

Emits a drop-and-re-add pair per feature table whose key has no time-typed column, using the first candidate event-time column found. The generated statements are proposals: the operator must confirm the column choice and the nullability check per table before running any of them.

```sql
WITH pk AS (
    SELECT LOWER(tc.table_name) AS table_name, tc.constraint_name
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.tables t
      ON t.table_schema = tc.table_schema AND t.table_name = tc.table_name
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}')
      AND tc.constraint_type = 'PRIMARY KEY'
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
),
pk_cols AS (
    SELECT LOWER(k.table_name) AS table_name,
           array_join(transform(array_sort(collect_list(struct(k.ordinal_position, k.column_name))), x -> concat('`', x.column_name, '`')), ', ')
               AS key_list,
           MAX(CASE WHEN UPPER(c.data_type) IN ('TIMESTAMP', 'TIMESTAMP_NTZ', 'DATE') THEN 1 ELSE 0 END) AS has_time_key
    FROM {{ catalog }}.information_schema.key_column_usage k
    JOIN pk ON pk.table_name = LOWER(k.table_name) AND pk.constraint_name = k.constraint_name
    JOIN {{ catalog }}.information_schema.columns c
      ON c.table_schema = k.table_schema AND c.table_name = k.table_name AND c.column_name = k.column_name
    WHERE LOWER(k.table_schema) = LOWER('{{ schema }}')
    GROUP BY LOWER(k.table_name)
),
candidate AS (
    SELECT LOWER(c.table_name) AS table_name, MIN(c.column_name) AS ts_column
    FROM {{ catalog }}.information_schema.columns c
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND UPPER(c.data_type) IN ('TIMESTAMP', 'TIMESTAMP_NTZ', 'DATE')
      AND REGEXP_LIKE(LOWER(c.column_name), 'event|_at$|_ts$|_time$|timestamp|effective|valid_from|as_of')
    GROUP BY LOWER(c.table_name)
)
SELECT concat(
    'ALTER TABLE `{{ catalog }}`.`{{ schema }}`.`', pk.table_name, '` DROP PRIMARY KEY CASCADE;\n',
    'ALTER TABLE `{{ catalog }}`.`{{ schema }}`.`', pk.table_name, '` ALTER COLUMN `', cd.ts_column, '` SET NOT NULL;\n',
    'ALTER TABLE `{{ catalog }}`.`{{ schema }}`.`', pk.table_name, '` ADD CONSTRAINT `', pk.table_name,
    '_pk` PRIMARY KEY (', pc.key_list, ', `', cd.ts_column, '` TIMESERIES);'
) AS stmt
FROM pk
JOIN pk_cols   pc USING (table_name)
JOIN candidate cd USING (table_name)
WHERE pc.has_time_key = 0
ORDER BY pk.table_name
```

Tables with a time-typed key column but no flag are excluded here on purpose: for those the operator already knows the column and should use the single-table section, since the regenerated key would duplicate that column.

## Fix: Create new feature tables with the flag from the start

With the Feature Engineering client, the flag is a constructor argument:

```python
from databricks.feature_engineering import FeatureEngineeringClient
fe = FeatureEngineeringClient()
fe.create_table(
    name="{{ catalog }}.{{ schema }}.{{ asset }}",
    primary_keys=["{{ key_column }}", "{{ timestamp_column }}"],
    timeseries_columns=["{{ timestamp_column }}"],
    df=features_df,
    description="Point-in-time feature table",
)
```

In SQL:

```sql
CREATE TABLE {{ catalog }}.{{ schema }}.{{ asset }} (
    {{ key_column }}       STRING    NOT NULL,
    {{ timestamp_column }} TIMESTAMP NOT NULL,
    feature_1              DOUBLE,
    CONSTRAINT {{ asset }}_pk PRIMARY KEY ({{ key_column }}, {{ timestamp_column }} TIMESERIES)
)
```

## Organizational guidance

Require `timeseries_columns` in the feature-table creation template so a table cannot be registered without it, and add a CI step that runs `SHOW CREATE TABLE` on every table tagged `feature_table = 'true'` and fails when `TIMESERIES` is absent. Pair this with `temporal_referential_integrity` on the same column: a time-series key over a timestamp that is null, in the future or at the epoch gives point-in-time joins that are technically correct and practically wrong.
