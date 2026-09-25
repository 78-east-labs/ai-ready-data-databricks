# Diagnostic: relationship_declaration

Lists every base table in the schema with the FOREIGN KEYs it declares, the FOREIGN KEYs that reference it, and the undeclared relationships its column names suggest.

## Context

One row per table. `outgoing_fks` lists declared relationships from this table (`fk_name: col -> parent.col`), `incoming_fks` lists relationships that point at it, using the same format from the other side. `suggested_fks` lists columns on this table that share a name with a single-column PRIMARY KEY on another table in the schema but are not covered by a declared FK; these are the candidates the bulk fix in fix.md will emit, and the human review point is whether the name match means a real join.

`relationship_status`:

- `DECLARED`: participates in at least one FK, either side.
- `SUGGESTED_ONLY`: no FK, but at least one suggested relationship.
- `ISOLATED`: no FK and nothing suggested. Either a genuinely standalone table (a reference list, a staging table) or one whose join columns do not follow the schema's naming.

`has_pk` matters for the parent side: a table cannot be referenced until it declares a PRIMARY KEY, so an `ISOLATED` dimension without a PK needs `entity_identifier_declaration` first.

Sorted so undeclared tables come first, those with suggestions ahead of the isolated ones.

## SQL

```sql
WITH tables_in_scope AS (
    SELECT LOWER(t.table_name) AS table_name, t.table_owner
    FROM {{ catalog }}.information_schema.tables t
    WHERE LOWER(t.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
),
fk_edges AS (
    SELECT tc.constraint_name                        AS fk_name,
           LOWER(tc.table_schema)                    AS child_schema,
           LOWER(tc.table_name)                      AS child_table,
           LOWER(pk.table_schema)                    AS parent_schema,
           LOWER(pk.table_name)                      AS parent_table,
           array_join(transform(array_sort(collect_list(struct(kc.ordinal_position, kc.column_name))), s -> s.column_name), ',') AS child_cols,
           array_join(transform(array_sort(collect_list(struct(kp.ordinal_position, kp.column_name))), s -> s.column_name), ',') AS parent_cols
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.referential_constraints rc
      ON rc.constraint_schema = tc.constraint_schema AND rc.constraint_name = tc.constraint_name
    JOIN {{ catalog }}.information_schema.table_constraints pk
      ON pk.constraint_schema = rc.unique_constraint_schema AND pk.constraint_name = rc.unique_constraint_name
     AND pk.constraint_type = 'PRIMARY KEY'
    JOIN {{ catalog }}.information_schema.key_column_usage kc
      ON kc.constraint_schema = tc.constraint_schema AND kc.constraint_name = tc.constraint_name
     AND kc.table_schema = tc.table_schema AND kc.table_name = tc.table_name
    JOIN {{ catalog }}.information_schema.key_column_usage kp
      ON kp.constraint_schema = pk.constraint_schema AND kp.constraint_name = pk.constraint_name
     AND kp.table_schema = pk.table_schema AND kp.table_name = pk.table_name
     AND kp.ordinal_position = kc.ordinal_position
    WHERE tc.constraint_type = 'FOREIGN KEY'
    GROUP BY 1, 2, 3, 4, 5
),
outgoing AS (
    SELECT child_table AS table_name,
           array_sort(collect_list(concat(fk_name, ': ', child_cols, ' -> ', parent_schema, '.', parent_table, '.', parent_cols))) AS outgoing_fks
    FROM fk_edges WHERE child_schema = LOWER('{{ schema }}') GROUP BY child_table
),
incoming AS (
    SELECT parent_table AS table_name,
           array_sort(collect_list(concat(fk_name, ': ', child_schema, '.', child_table, '.', child_cols, ' -> ', parent_cols))) AS incoming_fks
    FROM fk_edges WHERE parent_schema = LOWER('{{ schema }}') GROUP BY parent_table
),
single_col_pk AS (
    SELECT LOWER(tc.table_name) AS parent_table, max(k.column_name) AS pk_column
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.key_column_usage k
      ON k.constraint_schema = tc.constraint_schema AND k.constraint_name = tc.constraint_name
     AND k.table_schema = tc.table_schema AND k.table_name = tc.table_name
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}') AND tc.constraint_type = 'PRIMARY KEY'
    GROUP BY LOWER(tc.table_name)
    HAVING COUNT(*) = 1
),
declared_child_cols AS (
    SELECT child_table AS table_name, LOWER(child_cols) AS column_name
    FROM fk_edges WHERE child_schema = LOWER('{{ schema }}') AND child_cols NOT LIKE '%,%'
),
suggested AS (
    SELECT LOWER(c.table_name) AS table_name,
           array_sort(collect_list(concat(c.column_name, ' -> ', p.parent_table, '.', p.pk_column))) AS suggested_fks
    FROM {{ catalog }}.information_schema.columns c
    JOIN single_col_pk p
      ON LOWER(c.column_name) = LOWER(p.pk_column) AND LOWER(c.table_name) <> p.parent_table
    LEFT JOIN declared_child_cols d
      ON d.table_name = LOWER(c.table_name) AND d.column_name = LOWER(c.column_name)
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND d.column_name IS NULL
    GROUP BY LOWER(c.table_name)
),
pk_tables AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_constraints
    WHERE LOWER(table_schema) = LOWER('{{ schema }}') AND constraint_type = 'PRIMARY KEY'
)
SELECT
    t.table_name,
    t.table_owner,
    pk.table_name IS NOT NULL                                    AS has_pk,
    COALESCE(size(o.outgoing_fks), 0)                            AS outgoing_fk_count,
    COALESCE(size(i.incoming_fks), 0)                            AS incoming_fk_count,
    o.outgoing_fks,
    i.incoming_fks,
    s.suggested_fks,
    CASE
        WHEN o.outgoing_fks IS NOT NULL OR i.incoming_fks IS NOT NULL THEN 'DECLARED'
        WHEN s.suggested_fks IS NOT NULL                              THEN 'SUGGESTED_ONLY'
        ELSE 'ISOLATED'
    END                                                          AS relationship_status
FROM tables_in_scope t
LEFT JOIN outgoing  o  USING (table_name)
LEFT JOIN incoming  i  USING (table_name)
LEFT JOIN suggested s  USING (table_name)
LEFT JOIN pk_tables pk USING (table_name)
ORDER BY
    CASE WHEN o.outgoing_fks IS NOT NULL OR i.incoming_fks IS NOT NULL THEN 2
         WHEN s.suggested_fks IS NOT NULL THEN 0 ELSE 1 END,
    t.table_name
```
