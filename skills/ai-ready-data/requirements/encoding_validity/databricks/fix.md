# Fix: encoding_validity

Repair or strip damaged text, and fix the ingestion so it stops arriving.

## Context

Encoding damage is almost always introduced at ingest: a CSV or JSON file read with the wrong `encoding` option, a JDBC source whose connection charset differs from the database, or an upstream service that double-encodes. The data fix only cleans what is already in the table. Without the ingest fix the next load reintroduces it.

Options, in order:

1. **Re-ingest the affected files with the right charset.** The only lossless option when the raw files still exist. Auto Loader and `read_files` take `encoding`/`charset` for CSV and JSON.
2. **Re-decode mojibake in place.** UTF-8 text that was decoded as Latin-1 (or Windows-1252) can be reversed exactly with `decode(encode(v, 'ISO-8859-1'), 'UTF-8')`. Lossless when the whole value was double-encoded; harmful when applied to a value that was not. Apply only where the diagnostic marks it repairable.
3. **Strip replacement and control characters.** Lossy: the original characters are already gone (U+FFFD) or were never text (control bytes). Changes string length and can change meaning; document it.
4. **Quarantine** rows whose text is unrecoverable and matters (a document body for RAG, a name for matching).

Every mutating statement is preceded by a blast-radius query and limited to rows that currently fail, so re-running is a no-op. `is_valid_utf8()` / `try_validate_utf8()` need DBR 16.1+.

## Fix: Blast radius

```sql
SELECT
    COUNT_IF(NOT is_valid_utf8({{ column }}))                                                        AS invalid_utf8,
    COUNT_IF(contains({{ column }}, chr(65533)) OR REGEXP_LIKE({{ column }}, '[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F]')) AS strip_candidates,
    COUNT_IF(REGEXP_LIKE({{ column }}, '(Ã[\\x80-\\xBF]|â€[\\x80-\\xBF™œž]|Â[\\xA0-\\xBF])'))          AS redecode_candidates,
    COUNT(*)                                                                                         AS non_null_rows
FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE {{ column }} IS NOT NULL
```

## Fix: Re-ingest with the correct charset

For the files identified by the diagnostic. `read_files` and Auto Loader accept the source encoding; Delta always stores UTF-8.

```sql
-- Preview: does the file decode cleanly as Windows-1252?
SELECT {{ column }}, is_valid_utf8({{ column }}) AS ok
FROM read_files('{{ raw_path }}', format => 'csv', header => true, encoding => 'windows-1252')
LIMIT 20;

-- Replace the affected rows (keyed on the raw file name captured at ingest)
MERGE INTO {{ catalog }}.{{ schema }}.{{ asset }} t
USING (
    SELECT *, _metadata.file_path AS source_file
    FROM read_files('{{ raw_path }}', format => 'csv', header => true, encoding => 'windows-1252')
) s
ON t.{{ key_column }} = s.{{ key_column }}
WHEN MATCHED AND NOT is_valid_utf8(t.{{ column }}) THEN UPDATE SET t.{{ column }} = s.{{ column }}
```

For Auto Loader pipelines set `.option("encoding", "windows-1252")` (CSV) or `.option("charset", ...)` (JSON) on the stream and let the next run backfill with `cloudFiles.allowOverwrites` if the files are re-dropped.

## Fix: Re-decode mojibake in place

Applies the exact inverse of "UTF-8 read as Latin-1". The guard in the `WHERE` re-decodes each candidate and only updates it if the result is clean UTF-8 without residual mojibake, so a value that was not double-encoded is left alone.

```sql
UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET {{ column }} = decode(encode({{ column }}, 'ISO-8859-1'), 'UTF-8')
WHERE {{ column }} IS NOT NULL
  AND REGEXP_LIKE({{ column }}, '(Ã[\\x80-\\xBF]|â€[\\x80-\\xBF™œž]|Â[\\xA0-\\xBF])')
  AND NOT REGEXP_LIKE({{ column }}, '[^\\x00-\\xFF]')
  AND is_valid_utf8(decode(encode({{ column }}, 'ISO-8859-1'), 'UTF-8'))
  AND NOT contains(decode(encode({{ column }}, 'ISO-8859-1'), 'UTF-8'), chr(65533))
```

`NOT REGEXP_LIKE(col, '[^\x00-\xFF]')` excludes values containing characters above U+00FF, which `encode(..., 'ISO-8859-1')` cannot represent. Values double-encoded through Windows-1252 rather than Latin-1 (`â€™` for the right single quote) round-trip through `'windows-1252'` instead; run the statement once per charset. Preview with a `SELECT` of the before and after on 50 rows before running the `UPDATE`.

## Fix: Strip replacement and control characters

Lossy. Removes U+FFFD and C0 controls other than TAB, LF, CR; collapses the whitespace that stripping can leave behind.

```sql
UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET {{ column }} = NULLIF(TRIM(regexp_replace(
        replace({{ column }}, chr(65533), ''),
        '[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F]', '')), '')
WHERE {{ column }} IS NOT NULL
  AND (contains({{ column }}, chr(65533))
       OR REGEXP_LIKE({{ column }}, '[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F]'))
```

`NULLIF(..., '')` turns a value that was nothing but garbage into NULL rather than an empty string. For invalid UTF-8 byte sequences (not U+FFFD), `try_validate_utf8({{ column }})` returns NULL for the whole value; if you would rather keep the good bytes, `make_valid_utf8({{ column }})` (DBR 16.1+, same release as `is_valid_utf8`) replaces each bad sequence with U+FFFD, after which the strip above applies. Run it as a separate `UPDATE ... WHERE NOT is_valid_utf8({{ column }})` first.

## Fix: Quarantine unrecoverable rows

```sql
CREATE TABLE IF NOT EXISTS {{ catalog }}.{{ schema }}.{{ asset }}_encoding_quarantine
AS SELECT *, '' AS bad_column, current_timestamp() AS quarantined_at
   FROM {{ catalog }}.{{ schema }}.{{ asset }} WHERE 1 = 0;

INSERT INTO {{ catalog }}.{{ schema }}.{{ asset }}_encoding_quarantine
SELECT s.*, '{{ column }}', current_timestamp()
FROM {{ catalog }}.{{ schema }}.{{ asset }} s
WHERE s.{{ column }} IS NOT NULL
  AND NOT is_valid_utf8(s.{{ column }})
  AND NOT EXISTS (
      SELECT 1 FROM {{ catalog }}.{{ schema }}.{{ asset }}_encoding_quarantine q
      WHERE q.{{ key_column }} = s.{{ key_column }} AND q.bad_column = '{{ column }}'
  );
```

Delete from the source only if the owner agrees; often the right move is to leave the row and NULL the column.

## Fix: Bulk generation of blast-radius queries for every string column

Emits one aggregate query per STRING column in the table so you can find which columns are affected without writing each by hand.

```sql
SELECT concat(
    'SELECT ''', column_name, ''' AS column_name, COUNT(*) AS non_null_rows, ',
    'COUNT_IF(NOT is_valid_utf8(`', column_name, '`) OR contains(`', column_name, '`, chr(65533)) ',
    'OR REGEXP_LIKE(`', column_name, '`, ''[\\\\x00-\\\\x08\\\\x0B\\\\x0C\\\\x0E-\\\\x1F]'')) AS bad_rows ',
    'FROM {{ catalog }}.{{ schema }}.`{{ asset }}` WHERE `', column_name, '` IS NOT NULL'
) AS stmt
FROM {{ catalog }}.information_schema.columns
WHERE LOWER(table_schema) = LOWER('{{ schema }}')
  AND LOWER(table_name)   = LOWER('{{ asset }}')
  AND data_type = 'STRING'
ORDER BY ordinal_position
```

Join the emitted statements with `UNION ALL` to run them as one scan. The quadruple backslashes are two levels of escaping: this literal produces `\\x00` in the emitted text, which the emitted query's own parser turns into `\x00` for the regex engine.

## Organizational guidance

Declare the charset at every boundary. Auto Loader and `read_files` should always pass `encoding` explicitly for CSV and JSON, even when it is `UTF-8`, so a change in the producer shows up as a decode failure (`badRecordsPath` or `_rescued_data`) rather than as silent mojibake. Add `EXPECT (is_valid_utf8(col) AND NOT contains(col, chr(65533)))` expectations on text columns in bronze-to-silver flows for tables that feed embeddings or LLM prompts; a replacement character in a chunk is a permanent hole in the index. When a source system cannot say what charset it emits, treat that as a defect in the source contract and escalate; guessing per file is not a process.
