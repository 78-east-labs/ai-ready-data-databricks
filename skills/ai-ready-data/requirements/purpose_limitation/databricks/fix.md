# Fix: purpose_limitation

Record, per table, which AI processing purposes are permitted, with the `ai_allowed_purposes` tag, and where it matters turn the declaration into an enforced control.

## Context

`ai_allowed_purposes` states a decision made by the data owner, with privacy input where personal data is involved: for what AI uses may this table be read. It is derived from why the data was collected and on what legal basis, which is why `consent_coverage` usually has to be answered first. Applying a default (`analytics`, or worse `training`) across a schema to make the score green is not a fix; it manufactures permissions nobody granted. An unset tag is honest. If a bulk statement is run at all, the only defensible default is `none`, which withholds AI use until someone decides, and even that should be agreed with the owners because it may block pipelines that were legitimately running.

Vocabulary (keep it small, comma-separated, lowercase): `training`, `fine_tuning`, `evaluation`, `rag`, `analytics`, `feature_engineering`, `agent_tools`, `none`. Add `ai_purpose_ref` pointing to the decision record when one exists.

The tag alone does not stop anything. Enforcement options on Databricks, from lightest to strictest:

- A pre-flight query in training and indexing pipelines that refuses sources whose tag lacks the pipeline's purpose.
- Grants: put AI service principals in purpose-specific groups and only grant `SELECT` on tables whose tag includes that purpose (a job can reconcile grants from tags nightly).
- A row filter that returns no rows to members of an AI group unless the table's declared purposes include theirs. Row filters cannot read tags at runtime, so the purposes are passed as a function argument bound at `SET ROW FILTER` time, which must be re-run when the tag changes.

Tags need `APPLY TAG` or ownership; `SET TAGS` is idempotent for the same value. Governed tag policies may constrain values; check them first.

## Fix: Tag one table after the owner's decision

Guard:

```sql
SELECT tag_value
FROM {{ catalog }}.information_schema.table_tags
WHERE LOWER(schema_name) = LOWER('{{ schema }}')
  AND LOWER(table_name)  = LOWER('{{ asset }}')
  AND LOWER(tag_name)    = 'ai_allowed_purposes'
```

Skip if the same value is present; if a different value is present someone already decided, confirm before overwriting.

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TAGS ('ai_allowed_purposes' = '{{ ai_allowed_purposes }}',
          'ai_purpose_ref'      = '{{ ai_purpose_ref }}');
```

`{{ ai_allowed_purposes }}` is a comma-separated subset of the vocabulary, for example `rag,analytics`. Drop the second pair if there is no decision record; do not invent one.

## Fix: Generate statements from a purpose register

`{{ purpose_register }}` has one row per decided table: `table_name STRING`, `ai_allowed_purposes STRING`, `ai_purpose_ref STRING`, `decided_by STRING`, `decided_on DATE`. Emits statements for base tables that are untagged or disagree with the register, after validating the value against the vocabulary.

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
current_tag AS (
    SELECT LOWER(table_name) AS table_name, MAX(tag_value) AS ai_allowed_purposes
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = 'ai_allowed_purposes'
    GROUP BY LOWER(table_name)
),
register AS (
    SELECT LOWER(table_name) AS table_name,
           array_join(array_sort(transform(split(LOWER(ai_allowed_purposes), '\\s*,\\s*'), x -> trim(x))), ',') AS purposes,
           ai_purpose_ref, decided_by, decided_on
    FROM {{ purpose_register }}
    WHERE forall(split(LOWER(ai_allowed_purposes), '\\s*,\\s*'),
                 p -> array_contains(split('{{ purpose_vocabulary }}', ','), trim(p)))
)
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', t.table_name,
    '` SET TAGS (''ai_allowed_purposes'' = ''', r.purposes, '''',
    CASE WHEN r.ai_purpose_ref IS NOT NULL
         THEN concat(', ''ai_purpose_ref'' = ''', r.ai_purpose_ref, '''') ELSE '' END,
    ');'
) AS stmt,
r.decided_by, r.decided_on
FROM tables_in_scope t
JOIN register r USING (table_name)
LEFT JOIN current_tag c USING (table_name)
WHERE c.ai_allowed_purposes IS NULL OR LOWER(c.ai_allowed_purposes) <> r.purposes
ORDER BY t.table_name
```

Show the statements to the user before running them. Tables not in the register are the owners' worklist.

## Fix: Withhold AI use by default on personal-data tables (only with owner agreement)

If the governance team decides that undeclared personal-data tables must be closed to AI until reviewed, this emits `none` for tables that have PII-tagged columns and no declaration. It is a policy decision, not a technical default; run it only when that policy exists in writing, and pair it with the enforcement below or it changes nothing in practice.

```sql
WITH personal_data_tables AS (
    SELECT DISTINCT LOWER(ct.table_name) AS table_name
    FROM {{ catalog }}.information_schema.column_tags ct
    JOIN {{ catalog }}.information_schema.tables t
      ON  LOWER(t.table_schema) = LOWER(ct.schema_name)
      AND LOWER(t.table_name)   = LOWER(ct.table_name)
    WHERE LOWER(ct.schema_name) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND LOWER(ct.tag_name) = LOWER('{{ pii_tag_key }}')
),
declared AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = 'ai_allowed_purposes'
)
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', p.table_name,
    '` SET TAGS (''ai_allowed_purposes'' = ''none'', ''ai_purpose_ref'' = ''pending_review'');'
) AS stmt
FROM personal_data_tables p
LEFT JOIN declared d USING (table_name)
WHERE d.table_name IS NULL
ORDER BY p.table_name
```

## Fix: Enforce a purpose with a row filter

Members of `{{ ai_group }}` (for example `ml_training_sp`) see rows only if `{{ purpose }}` is among the purposes bound to the table; everyone else is unaffected. One filter per table is allowed, so run the guard and skip tables that already have a filter (combine logic into that function instead).

Guard for the function and for an existing filter:

```sql
SELECT routine_name FROM {{ catalog }}.information_schema.routines
WHERE LOWER(routine_schema) = LOWER('{{ governance_schema }}') AND LOWER(routine_name) = 'rf_ai_purpose';

SELECT filter_name FROM {{ catalog }}.information_schema.row_filters
WHERE LOWER(schema_name) = LOWER('{{ schema }}') AND LOWER(table_name) = LOWER('{{ asset }}');
```

```sql
CREATE FUNCTION {{ catalog }}.{{ governance_schema }}.rf_ai_purpose(allowed_purposes STRING)
RETURNS BOOLEAN
RETURN CASE
    WHEN NOT is_account_group_member('{{ ai_group }}') THEN TRUE
    ELSE array_contains(split(LOWER(allowed_purposes), '\\s*,\\s*'), '{{ purpose }}')
END;

ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET ROW FILTER {{ catalog }}.{{ governance_schema }}.rf_ai_purpose ON ('{{ ai_allowed_purposes }}');
```

The literal passed in `ON (...)` is a snapshot of the tag at attach time. Re-run the `SET ROW FILTER` whenever `ai_allowed_purposes` changes; the register-driven generator above can emit both statements together. Verify from a warehouse as a member of `{{ ai_group }}`: `SELECT COUNT(*) FROM {{ catalog }}.{{ schema }}.{{ asset }}` returns 0 on a table whose purposes exclude `{{ purpose }}`.

## Fix: Pre-flight check for AI pipelines

Cheapest enforcement, no DDL: the first cell of a training, fine-tuning or indexing job asserts every source is cleared for its purpose. Returns the offending tables; the job fails if any row comes back.

```sql
WITH sources AS (
    SELECT explode(split('{{ source_tables }}', ',')) AS full_name   -- 'cat.sch.t1,cat.sch.t2'
),
declared AS (
    SELECT LOWER(concat_ws('.', catalog_name, schema_name, table_name)) AS full_name,
           transform(split(LOWER(tag_value), '\\s*,\\s*'), x -> trim(x)) AS purposes
    FROM system.information_schema.table_tags
    WHERE LOWER(tag_name) = 'ai_allowed_purposes'
)
SELECT s.full_name,
       d.purposes,
       CASE WHEN d.full_name IS NULL THEN 'UNDECLARED' ELSE 'PURPOSE_NOT_ALLOWED' END AS reason
FROM sources s
LEFT JOIN declared d ON LOWER(trim(s.full_name)) = d.full_name
WHERE d.full_name IS NULL OR NOT array_contains(d.purposes, '{{ purpose }}')
```

## Organizational guidance

Purpose is decided at intake alongside legal basis and retention, by the data owner, and written to one register the tags are generated from. Make `ai_allowed_purposes` a governed tag with the vocabulary as its allowed values so free text cannot creep in. Give every AI workload a declared purpose (job tag or `query_tags`) and make the pre-flight assertion part of the pipeline template; that closes the loop between declaration and use without a large row-filter estate. Review declarations when the legal basis changes or when the diagnostic's contradiction query shows AI consumers reading tables tagged `none`.
