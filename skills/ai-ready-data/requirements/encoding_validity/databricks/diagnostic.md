# Diagnostic: encoding_validity

Counts each kind of encoding defect in the column, shows sample offending values with their byte-level detail, and locates the files or commits the bad rows came from.

## Context

Three queries:

1. **Defect breakdown.** How many non-null values fail for each reason (invalid UTF-8, U+FFFD, control characters, mojibake), and how many would be repaired by re-decoding. The reasons overlap; a value can hit several.
2. **Sample offending rows.** Up to 100 values with the issue label, the character length versus the byte length (a large gap on ASCII-looking text is a tell), a hex dump of the first 32 bytes, and the source file from `_metadata.file_path` (available on every Delta table read; needs no configuration). One file with all the bad rows is a one-off; many files across time is a producer defect.
3. **Distribution by file and commit.** Bad-row counts per source file, and per commit version when Change Data Feed is on, so the fix can be scoped to the affected load.

`is_valid_utf8()` and `try_validate_utf8()` need DBR 16.1+; on older runtimes drop those expressions and rely on the other three signals.

## SQL

### Defect breakdown

```sql
WITH vals AS (
    SELECT {{ column }} AS v
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
    WHERE {{ column }} IS NOT NULL
)
SELECT
    '{{ asset }}'                                                          AS table_name,
    '{{ column }}'                                                         AS column_name,
    COUNT(*)                                                               AS non_null_rows,
    COUNT_IF(NOT is_valid_utf8(v))                                         AS invalid_utf8,
    COUNT_IF(contains(v, chr(65533)))                                      AS has_replacement_char,
    COUNT_IF(REGEXP_LIKE(v, '[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F]'))          AS has_control_chars,
    COUNT_IF(REGEXP_LIKE(v, '(Ã[\\x80-\\xBF]|â€[\\x80-\\xBF™œž]|Â[\\xA0-\\xBF])')) AS looks_like_mojibake,
    COUNT_IF(REGEXP_LIKE(v, '(Ã[\\x80-\\xBF]|â€[\\x80-\\xBF™œž]|Â[\\xA0-\\xBF])')
             AND NOT REGEXP_LIKE(decode(encode(v, 'ISO-8859-1'), 'UTF-8'), '(Ã[\\x80-\\xBF]|â€|\\uFFFD)')) AS mojibake_repairable,
    COUNT_IF(contains(v, chr(0)))                                          AS has_null_byte
FROM vals
```

`mojibake_repairable` re-decodes the value as if it had been UTF-8 misread as Latin-1 and checks whether the result is clean. That is the exact repair the fix offers, so this number is the fix's expected effect. `encode(v, 'ISO-8859-1')` fails on characters above U+00FF; if the column mixes languages, wrap it in `try_` logic per the fix.

### Sample offending rows

```sql
SELECT
    {{ key_columns }},
    {{ column }}                                                            AS problematic_value,
    length({{ column }})                                                    AS char_length,
    octet_length({{ column }})                                              AS byte_length,
    hex(substring(encode({{ column }}, 'UTF-8'), 1, 32))                    AS first_32_bytes_hex,
    CASE
        WHEN NOT is_valid_utf8({{ column }})                                THEN 'INVALID_UTF8'
        WHEN contains({{ column }}, chr(0))                                 THEN 'NULL_BYTE'
        WHEN contains({{ column }}, chr(65533))                             THEN 'REPLACEMENT_CHAR'
        WHEN REGEXP_LIKE({{ column }}, '[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F]') THEN 'CONTROL_CHARS'
        ELSE 'MOJIBAKE'
    END                                                                     AS issue,
    _metadata.file_path                                                     AS source_file
FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE {{ column }} IS NOT NULL
  AND (
       NOT is_valid_utf8({{ column }})
    OR contains({{ column }}, chr(65533))
    OR REGEXP_LIKE({{ column }}, '[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F]')
    OR REGEXP_LIKE({{ column }}, '(Ã[\\x80-\\xBF]|â€[\\x80-\\xBF™œž]|Â[\\xA0-\\xBF])')
  )
ORDER BY issue, byte_length DESC
LIMIT 100
```

`{{ key_columns }}` is the table's identifier (default `*` is too wide here; pick the key). `_metadata.file_path` is the current Delta data file, which changes after `OPTIMIZE`; it still groups bad rows together because rewrites preserve locality.

### Distribution by source file and commit

```sql
SELECT
    _metadata.file_path                                     AS source_file,
    _metadata.file_modification_time                        AS file_written_at,
    COUNT(*)                                                AS rows_in_file,
    COUNT_IF(
        NOT is_valid_utf8({{ column }})
        OR contains({{ column }}, chr(65533))
        OR REGEXP_LIKE({{ column }}, '[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F]')
    )                                                       AS bad_rows
FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE {{ column }} IS NOT NULL
GROUP BY _metadata.file_path, _metadata.file_modification_time
HAVING bad_rows > 0
ORDER BY bad_rows DESC
LIMIT 100
```

If the table was loaded by Auto Loader and kept its `_metadata` or `source_file` column from ingest, group by that instead; it names the original raw file rather than the Delta file. With Change Data Feed enabled, `table_changes('{{ catalog }}.{{ schema }}.{{ asset }}', 0)` grouped by `_commit_version` with the same predicate identifies the commit that introduced the damage, and `DESCRIBE HISTORY` names the job.
