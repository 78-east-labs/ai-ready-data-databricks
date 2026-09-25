# Fix: chunk_readiness

Produce a chunk table next to each long-text table, with a stable chunk id, a parent reference and overlapping fixed-size chunks; or enrich short rows with context.

## Context

Chunking is a data transformation, not a property change, so the fix creates a **new** table rather than altering the source. The source stays untouched; the chunk table becomes the input for embeddings and Vector Search. This keeps the check's structural signal truthful (a `chunk_index` column exists because a chunking step exists) and keeps the source usable for everything else.

Design choices baked into the SQL below, override as needed:

- Chunk length `{{ max_chunk_chars }}` (default 4000) with a 10 percent overlap (`{{ chunk_overlap_chars }}`, default 400). Overlap keeps a sentence that straddles a boundary retrievable from either side.
- Fixed-width character chunks. They are predictable and cheap. Sentence- or heading-aware splitting is better for prose; do that in a Python step (Lakeflow pipeline with a text splitter) and write the same output schema.
- Output columns: `chunk_id` (deterministic, `sha2(parent_id || ':' || chunk_index)`), `{{ key_column }}` as the parent reference, `chunk_index`, `chunk_text`, `chunk_chars`, `source_updated_at`. Vector Search Delta Sync indexes need a primary key column; `chunk_id` is it.
- Change Data Feed is enabled at creation so a Delta Sync index can follow the table incrementally.

Never `CREATE OR REPLACE TABLE`. The pattern is `CREATE TABLE IF NOT EXISTS` once, then `MERGE` on every refresh, which makes the fix idempotent and preserves the chunk table's history and any index built on it.

Placeholders: `{{ key_column }}` (the source's primary key, from `key_column_usage` when a PK constraint exists), `{{ column }}` (the text column), `{{ chunk_overlap_chars }}` (default 400), `{{ updated_at_column }}` (optional, a source timestamp; use `current_timestamp()` if none).

Permissions: `CREATE TABLE` on the schema, `SELECT` on the source, `MODIFY` on the chunk table.

## Fix: Create the chunk table

Blast radius first: how many rows and chunks will this produce?

```sql
SELECT
    COUNT(*)                                                             AS source_rows,
    SUM(GREATEST(1, CEIL((length({{ column }}) - {{ chunk_overlap_chars }})
        / ({{ max_chunk_chars }} - {{ chunk_overlap_chars }}))))         AS expected_chunks
FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE {{ column }} IS NOT NULL AND length({{ column }}) > 0
```

Then create the table (no-op when it already exists):

```sql
CREATE TABLE IF NOT EXISTS {{ catalog }}.{{ schema }}.{{ asset }}_chunks (
    chunk_id          STRING NOT NULL,
    {{ key_column }}  STRING NOT NULL,
    chunk_index       INT    NOT NULL,
    chunk_text        STRING NOT NULL,
    chunk_chars       INT,
    source_updated_at TIMESTAMP,
    CONSTRAINT {{ asset }}_chunks_pk PRIMARY KEY (chunk_id)
)
COMMENT 'Fixed-width chunks of {{ catalog }}.{{ schema }}.{{ asset }}.{{ column }}, {{ max_chunk_chars }} chars with {{ chunk_overlap_chars }} overlap'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.enableDeletionVectors' = 'true'
)
CLUSTER BY ({{ key_column }});
```

If `{{ key_column }}` is not a string, change its declared type to match the source.

## Fix: Populate or refresh the chunks

Idempotent: re-running merges changed text and inserts new rows. Chunks whose parent row no longer exists are deleted so the chunk table does not drift from the source.

```sql
MERGE INTO {{ catalog }}.{{ schema }}.{{ asset }}_chunks AS tgt
USING (
    WITH src AS (
        SELECT
            CAST({{ key_column }} AS STRING)                    AS parent_id,
            {{ column }}                                        AS full_text,
            length({{ column }})                                AS total_chars,
            {{ updated_at_column }}                             AS source_updated_at
        FROM {{ catalog }}.{{ schema }}.{{ asset }}
        WHERE {{ column }} IS NOT NULL AND length({{ column }}) > 0
    ),
    exploded AS (
        SELECT
            parent_id,
            source_updated_at,
            idx                                                 AS chunk_index,
            substring(full_text,
                      idx * ({{ max_chunk_chars }} - {{ chunk_overlap_chars }}) + 1,
                      {{ max_chunk_chars }})                    AS chunk_text
        FROM src
        LATERAL VIEW explode(sequence(
            0,
            GREATEST(0, CAST(CEIL((total_chars - {{ chunk_overlap_chars }})
                / ({{ max_chunk_chars }} - {{ chunk_overlap_chars }})) AS INT) - 1)
        )) s AS idx
    )
    SELECT
        sha2(concat(parent_id, ':', chunk_index), 256) AS chunk_id,
        parent_id                                       AS {{ key_column }},
        chunk_index,
        chunk_text,
        length(chunk_text)                              AS chunk_chars,
        source_updated_at
    FROM exploded
    WHERE length(trim(chunk_text)) > 0
) AS src
ON tgt.chunk_id = src.chunk_id
WHEN MATCHED AND tgt.chunk_text <> src.chunk_text THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;
```

Verify with the check's data variant against `{{ asset }}_chunks.chunk_text`; p95 should be at or below `{{ max_chunk_chars }}` by construction.

## Fix: Enrich short rows with context

For tables where the diagnostic shows most rows in `TOO_SHORT (<100)`, embedding the bare text produces weak vectors. A view that prepends the title or category costs nothing and gives the embedding model something to work with. Views are safe to `CREATE OR REPLACE`; they hold no data.

```sql
CREATE OR REPLACE VIEW {{ catalog }}.{{ schema }}.{{ asset }}_enriched AS
SELECT
    {{ key_column }},
    concat_ws('\n',
        concat('Title: ', {{ title_column }}),
        {{ column }}
    ) AS enriched_text
FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE {{ column }} IS NOT NULL;
```

Vector Search Delta Sync indexes cannot be built on a view; materialize it as a table (same `CREATE TABLE IF NOT EXISTS` + `MERGE` pattern as above) when it becomes an index source.

## Fix: Bulk-generate chunk tables for every RAW_TEXT table

Feed the diagnostic's `RAW_TEXT` rows in as a temp view `raw_text_tables(table_name, text_column, key_column)` and generate the DDL. Populate statements are generated the same way by substituting into the `MERGE` above.

```sql
SELECT concat(
    'CREATE TABLE IF NOT EXISTS {{ catalog }}.{{ schema }}.`', table_name, '_chunks` (',
    'chunk_id STRING NOT NULL, `', key_column, '` STRING NOT NULL, chunk_index INT NOT NULL, ',
    'chunk_text STRING NOT NULL, chunk_chars INT, source_updated_at TIMESTAMP, ',
    'CONSTRAINT `', table_name, '_chunks_pk` PRIMARY KEY (chunk_id)) ',
    'TBLPROPERTIES (''delta.enableChangeDataFeed'' = ''true'') ',
    'CLUSTER BY (`', key_column, '`);'
) AS stmt
FROM raw_text_tables
ORDER BY table_name
```

Show the generated statements to the user before executing them.

## Organizational guidance

Make chunking a pipeline stage with a shared output schema (`chunk_id`, parent key, `chunk_index`, `chunk_text`) rather than something each RAG project does in its notebook. A Lakeflow Declarative Pipeline that reads the bronze document table, parses binaries with `ai_parse_document()`, splits with a sentence-aware splitter and writes `*_chunks` tables gives every downstream index the same input and makes the check pass by construction. Record the chunk size and overlap in the table comment so a model change (larger window, different tokenizer) can be re-chunked deliberately.
