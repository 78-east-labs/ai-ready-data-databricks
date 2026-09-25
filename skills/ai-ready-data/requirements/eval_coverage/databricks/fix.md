# Fix: eval_coverage

Link existing evaluation sets to the tables they exercise with the `{{ eval_tag_key }}` tag, and create an MLflow evaluation dataset in Unity Catalog for tables that have none.

## Context

Two situations, with different fixes:

- **The eval set exists but is not linked.** The diagnostic's `LINKED_BY_NAME` rows and the inventory's `UNTAGGED` artifacts. The fix is a tag on the eval table naming its target. The tag documents a fact a human must confirm (this dataset does test that table); applying it by pattern-matching names without checking is worse than leaving it off, because a false link hides a real gap.
- **No eval set exists.** `NO_EVAL` rows. Creating one is a design task for the team that owns the table: which questions or queries an agent will run against it, what the correct answers are, which edge cases matter. The SQL and Python below give the container; the content is theirs.

An MLflow evaluation dataset in UC is the native option. It is a Delta table with a fixed schema (`dataset_record_id`, `inputs`, `expectations`, `source_type`, `source_id`, `tags`, `create_time`, `created_by`, `last_update_time`, `last_updated_by`), is versioned through Delta history, and plugs into `mlflow.genai.evaluate()`. The check recognizes it by the `inputs` and `expectations` columns.

Permissions: `APPLY TAG` on the eval table (or ownership); `CREATE TABLE` on the schema for a new dataset.

## Fix: Tag an existing eval table with its target

Guard: `SELECT tag_value FROM {{ catalog }}.information_schema.table_tags WHERE LOWER(schema_name) = LOWER('{{ eval_schema }}') AND LOWER(table_name) = LOWER('{{ eval_asset }}') AND tag_name = '{{ eval_tag_key }}'`. If a value exists, append rather than overwrite.

```sql
ALTER TABLE {{ catalog }}.{{ eval_schema }}.{{ eval_asset }}
SET TAGS ('{{ eval_tag_key }}' = '{{ schema }}.{{ asset }}')
```

Multiple targets are a comma-separated list: `'{{ schema }}.orders,{{ schema }}.order_items'`. Governed tag policies may restrict `{{ eval_tag_key }}` values; check the policy before inventing a format.

## Fix: Create an MLflow evaluation dataset for a table

Python, run in a notebook or job with `mlflow>=3.0` and `databricks-agents` installed. Creating the dataset is idempotent by name: `create_dataset` fails if the UC table exists, so the snippet fetches it in that case.

```python
import mlflow
from mlflow.genai import datasets

mlflow.set_registry_uri("databricks-uc")
name = "{{ catalog }}.{{ schema }}.{{ asset }}_eval"
try:
    ds = datasets.create_dataset(uc_table_name=name)
except Exception:
    ds = datasets.get_dataset(uc_table_name=name)

ds.merge_records([
    {"inputs": {"question": "How many orders shipped last week?"},
     "expectations": {"expected_response": "SELECT COUNT(*) ... WHERE shipped_at >= ...",
                      "notes": "date range boundary"}},
    {"inputs": {"question": "Which order has id 0?"},
     "expectations": {"expected_response": "No such order", "notes": "missing key"}},
])
```

Then tag it (the name convention already links it, the tag makes the link explicit):

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}_eval
SET TAGS ('{{ eval_tag_key }}' = '{{ schema }}.{{ asset }}')
```

## Fix: Create a plain eval table in SQL

When MLflow is not in the picture, a conventional table with the same intent. `CREATE TABLE IF NOT EXISTS` keeps it idempotent; never `CREATE OR REPLACE`.

```sql
CREATE TABLE IF NOT EXISTS {{ catalog }}.{{ schema }}.{{ asset }}_eval (
    eval_id          STRING NOT NULL,
    inputs           STRING NOT NULL COMMENT 'Question, query or input record as JSON',
    expectations     STRING NOT NULL COMMENT 'Expected answer, rows or behaviour as JSON',
    category         STRING COMMENT 'happy_path | edge | null_handling | adversarial',
    notes            STRING,
    created_at       TIMESTAMP DEFAULT current_timestamp(),
    CONSTRAINT {{ asset }}_eval_pk PRIMARY KEY (eval_id)
)
COMMENT 'Evaluation set for {{ catalog }}.{{ schema }}.{{ asset }}'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}_eval
SET TAGS ('{{ eval_tag_key }}' = '{{ schema }}.{{ asset }}');
```

Seed it deliberately rather than randomly: one record per status value, both ends of each numeric range, null-bearing rows, and the tricky records that other diagnostics flagged. 100 to a few thousand records is the useful range.

## Fix: Bulk-generate tags for eval artifacts linked by name only

Feed the diagnostic's `LINKED_BY_NAME` rows in as a temp view `by_name_links(table_name, eval_table)` after a human has confirmed each pair.

```sql
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', eval_table,
    '` SET TAGS (''{{ eval_tag_key }}'' = ''{{ schema }}.', table_name, ''');'
) AS stmt
FROM by_name_links
ORDER BY eval_table
```

Show the generated statements to the user before executing them.

## Organizational guidance

Eval sets pay off when they gate changes. Store them in UC next to the data they test, tag them, and run `mlflow.genai.evaluate()` against them in the agent's CI job so a regression fails the build rather than a customer conversation. Make "has an eval set" part of the definition of done for any table an agent or Genie space reads, and review the inventory query monthly for `DANGLING` tags after renames.
