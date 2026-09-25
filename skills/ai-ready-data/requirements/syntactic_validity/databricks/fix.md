# Fix: syntactic_validity

Repair the mechanically repairable payloads, quarantine the rest, and move parsing to ingest so malformed records are counted there instead of stored as strings.

## Context

Which repair applies depends on the cause from the diagnostic:

- **Quote-wrapped document, single quotes, trailing commas, `NaN`/`Infinity` tokens**: mechanical, reversible text transforms whose result can be verified with `try_parse_json` before it is written. The fix statements below apply each transform only where the original fails and the transformed value parses.
- **Truncated payloads**: not repairable. The bytes are gone. Quarantine, and fix the cap in the producer or transport.
- **Not JSON at all**: the column's contract is wrong or the producer mixed formats. Quarantine, and split the column or the source.
- **Rescued or corrupt rows from ingest**: handled under `schema_conformity` (promote fields, widen types) or by re-ingesting the affected files with the right options (`multiLine`, `quote`, `escape`, `sep` for CSV; `allowSingleQuotes`, `allowUnquotedControlChars` for JSON).

The durable fix is to parse at ingest into a VARIANT (or a typed STRUCT) column with rescue enabled, so a malformed record becomes a rescued row with a count in the pipeline event log rather than a string nobody can read.

Every mutating statement is preceded by the blast-radius query, restricted to rows that fail now and would pass after the transform, and therefore idempotent. `try_parse_json` needs DBR 15.3+. Deleted-file retention (7 days by default) is the undo window; the quarantine table keeps the originals regardless.

## Fix: Blast radius

```sql
SELECT
    COUNT_IF(try_parse_json({{ column }}) IS NULL)                                                       AS invalid_rows,
    COUNT_IF(try_parse_json({{ column }}) IS NULL
             AND try_parse_json(regexp_replace(TRIM({{ column }}), '^"(.*)"$', '$1')) IS NOT NULL)       AS fixable_quote_wrapped,
    COUNT_IF(try_parse_json({{ column }}) IS NULL
             AND try_parse_json(regexp_replace({{ column }}, ',\\s*([}\\]])', '$1')) IS NOT NULL)          AS fixable_trailing_comma,
    COUNT_IF(try_parse_json({{ column }}) IS NULL
             AND try_parse_json(replace({{ column }}, '''', '"')) IS NOT NULL)                            AS fixable_single_quotes,
    COUNT_IF(try_parse_json({{ column }}) IS NULL
             AND try_parse_json(regexp_replace({{ column }}, '(?<=[:\\[,]\\s{0,4})(NaN|-?Infinity)', 'null')) IS NOT NULL) AS fixable_nan_tokens,
    COUNT(*)                                                                                             AS non_null_rows
FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE {{ column }} IS NOT NULL
```

## Fix: Quarantine every invalid payload first

Keeps the originals with their keys so any repair can be audited and any unrecoverable record can be re-requested from the producer.

```sql
CREATE TABLE IF NOT EXISTS {{ catalog }}.{{ schema }}.{{ asset }}_invalid_payloads (
    {{ key_column }}    STRING,
    column_name         STRING,
    original_value      STRING,
    data_file           STRING,
    quarantined_at      TIMESTAMP
);

INSERT INTO {{ catalog }}.{{ schema }}.{{ asset }}_invalid_payloads
SELECT CAST(s.{{ key_column }} AS STRING), '{{ column }}', s.{{ column }}, s._metadata.file_path, current_timestamp()
FROM {{ catalog }}.{{ schema }}.{{ asset }} s
WHERE s.{{ column }} IS NOT NULL
  AND try_parse_json(s.{{ column }}) IS NULL
  AND NOT EXISTS (
      SELECT 1 FROM {{ catalog }}.{{ schema }}.{{ asset }}_invalid_payloads q
      WHERE q.{{ key_column }} = CAST(s.{{ key_column }} AS STRING) AND q.column_name = '{{ column }}'
  );
```

## Fix: Unwrap quote-wrapped documents

```sql
UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET {{ column }} = regexp_replace(TRIM({{ column }}), '^"(.*)"$', '$1')
WHERE {{ column }} IS NOT NULL
  AND try_parse_json({{ column }}) IS NULL
  AND try_parse_json(regexp_replace(TRIM({{ column }}), '^"(.*)"$', '$1')) IS NOT NULL
```

## Fix: Remove trailing commas

```sql
UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET {{ column }} = regexp_replace({{ column }}, ',\\s*([}\\]])', '$1')
WHERE {{ column }} IS NOT NULL
  AND try_parse_json({{ column }}) IS NULL
  AND try_parse_json(regexp_replace({{ column }}, ',\\s*([}\\]])', '$1')) IS NOT NULL
```

## Fix: Convert single-quoted JSON

Only safe when the payload contains no legitimate apostrophes inside values; the parse guard rejects the cases where the naive replacement breaks the document, and those stay quarantined.

```sql
UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET {{ column }} = replace({{ column }}, '''', '"')
WHERE {{ column }} IS NOT NULL
  AND try_parse_json({{ column }}) IS NULL
  AND try_parse_json(replace({{ column }}, '''', '"')) IS NOT NULL
```

## Fix: Replace NaN and Infinity tokens with null

```sql
UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET {{ column }} = regexp_replace({{ column }}, '(?<=[:\\[,]\\s{0,4})(NaN|-?Infinity)', 'null')
WHERE {{ column }} IS NOT NULL
  AND try_parse_json({{ column }}) IS NULL
  AND try_parse_json(regexp_replace({{ column }}, '(?<=[:\\[,]\\s{0,4})(NaN|-?Infinity)', 'null')) IS NOT NULL
```

The lookbehind keeps the substitution to value positions, so the string `"NaN"` inside a quoted value is untouched.

## Fix: NULL unrecoverable payloads after quarantine

For truncated or non-JSON values, once they are in the quarantine table. Consumers then see a missing payload rather than a string that fails downstream.

```sql
UPDATE {{ catalog }}.{{ schema }}.{{ asset }} s
SET {{ column }} = NULL
WHERE s.{{ column }} IS NOT NULL
  AND try_parse_json(s.{{ column }}) IS NULL
  AND EXISTS (
      SELECT 1 FROM {{ catalog }}.{{ schema }}.{{ asset }}_invalid_payloads q
      WHERE q.{{ key_column }} = CAST(s.{{ key_column }} AS STRING)
        AND q.column_name = '{{ column }}'
  )
```

## Fix: Add a parsed VARIANT column alongside the string

Gives consumers a typed payload without changing the raw column. Backfill is idempotent; the pipeline should populate it going forward with `parse_json` (or `try_parse_json` plus an expectation).

```sql
-- Guard: information_schema.columns for '{{ column }}_v'
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ADD COLUMNS ({{ column }}_v VARIANT COMMENT 'Parsed form of {{ column }}; NULL where the payload is malformed');

UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET {{ column }}_v = try_parse_json({{ column }})
WHERE {{ column }}_v IS NULL AND {{ column }} IS NOT NULL;
```

VARIANT is a Delta table feature (writer version 7); readers older than DBR 15.3 cannot read the table after this. Check consumers first.

## Fix: Re-ingest affected files with corrected parser options

For rescue- or corrupt-record-shaped failures, the fix is the reader, not the table. Preview the options against one of the files the diagnostic named, then let the pipeline reprocess.

```sql
SELECT COUNT(*) AS rows, COUNT_IF(_rescued_data IS NOT NULL) AS still_rescued
FROM read_files('{{ raw_file_or_glob }}',
                format => 'json',
                multiLine => true,
                allowSingleQuotes => true,
                allowUnquotedControlChars => true,
                rescuedDataColumn => '_rescued_data')
```

For CSV: `sep`, `quote`, `escape`, `multiLine`, `mode => 'PERMISSIVE'`, `rescuedDataColumn`. When `still_rescued` reaches zero with the right options, apply them to the Auto Loader stream and reprocess the files (`cloudFiles.allowOverwrites = true` for the affected paths, or a targeted backfill with `MERGE` on the record key).

## Organizational guidance

Parse at the boundary. Land payloads as VARIANT with Auto Loader's rescue column enabled and an expectation on the parse (`CONSTRAINT payload_parses EXPECT (payload IS NOT NULL) ON VIOLATION DROP ROW` after `try_parse_json`), so malformed records are counted per run in the event log and never reach a STRING column that models or RAG chunkers read. Give every producer a schema contract (a JSON Schema or a Protobuf definition stored next to the pipeline) and a payload size limit that is the same at every hop; truncation always means two systems disagree on that number. When a column has to carry mixed formats for a transition, add a `payload_format` column so the parser is chosen deterministically instead of guessed.
