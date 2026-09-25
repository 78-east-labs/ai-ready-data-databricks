# Diagnostic: syntactic_validity

Classifies the malformed payloads by failure pattern, shows samples with the position where parsing likely broke, and finds the load that introduced them.

## Context

Three queries:

1. **Failure pattern breakdown.** Counts of non-parsing values by likely cause: truncated (unbalanced braces or brackets), a document wrapped in unescaped quotes (a naive `'"' || payload || '"'` somewhere upstream), single-quoted keys or values (Python `repr` style), trailing commas, unescaped control characters, plain text that is not JSON at all, and `NaN`/`Infinity` tokens. Each cause has a different fix; the breakdown tells you which one to write. A properly double-encoded payload (a JSON string whose content is JSON) parses successfully as a scalar and therefore does not fail this check; the object-or-array variant of the check catches it.
2. **Samples.** Up to 100 failing values, truncated to 300 characters, with length, the first character, the last character, brace balance and the cause label. The key columns locate the record.
3. **Onset by file and commit.** Failure counts per Delta data file with its write time, newest first. If Change Data Feed is on, the commit query attributes them to a version and `DESCRIBE HISTORY` to a job.

`{{ key_columns }}` is the table identifier (no default). Everything uses `try_parse_json` (DBR 15.3+); substitute `get_json_object(col, '$')` for older runtimes.

## SQL

### Failure pattern breakdown

```sql
WITH bad AS (
    SELECT {{ column }} AS v
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
    WHERE {{ column }} IS NOT NULL
      AND try_parse_json({{ column }}) IS NULL
),
labelled AS (
    SELECT
        v,
        CASE
            WHEN TRIM(v) = ''                                                              THEN 'EMPTY_STRING'
            WHEN v RLIKE '^\\s*"\\s*[\\[{]' AND v RLIKE '[}\\]]\\s*"\\s*$'                  THEN 'QUOTE_WRAPPED_DOCUMENT'
            WHEN v RLIKE '^\\s*[\\[{]' AND
                 (length(regexp_replace(v, '[^{]', '')) <> length(regexp_replace(v, '[^}]', ''))
               OR length(regexp_replace(v, '[^\\[]', '')) <> length(regexp_replace(v, '[^\\]]', ''))) THEN 'TRUNCATED_OR_UNBALANCED'
            WHEN v RLIKE '^\\s*[\\[{]' AND v RLIKE ',\\s*[}\\]]'                            THEN 'TRAILING_COMMA'
            WHEN v RLIKE '^\\s*[\\[{]' AND v RLIKE '''[^'']*''\\s*:'                        THEN 'SINGLE_QUOTED'
            WHEN v RLIKE '^\\s*[\\[{]' AND v RLIKE '(NaN|-?Infinity)'                       THEN 'NAN_OR_INFINITY_TOKEN'
            WHEN v RLIKE '^\\s*[\\[{]' AND v RLIKE '[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F]'       THEN 'UNESCAPED_CONTROL_CHAR'
            WHEN v RLIKE '^\\s*[\\[{]'                                                     THEN 'OTHER_MALFORMED_JSON'
            ELSE 'NOT_JSON_AT_ALL'
        END AS cause
    FROM bad
)
SELECT
    cause,
    COUNT(*)                                                AS rows,
    COUNT(*)::DOUBLE / SUM(COUNT(*)) OVER ()                AS share_of_failures,
    MIN(length(v))                                          AS min_length,
    MAX(length(v))                                          AS max_length,
    substring(MIN(v), 1, 120)                               AS sample
FROM labelled
GROUP BY cause
ORDER BY rows DESC
```

`TRUNCATED_OR_UNBALANCED` with `max_length` sitting at a round number (4000, 8000, 32767, 65535) is a size cap somewhere in the producer or a transport; that number is the finding.

### Samples

```sql
SELECT
    {{ key_columns }},
    substring({{ column }}, 1, 300)                         AS value_preview,
    length({{ column }})                                    AS char_length,
    substring(TRIM({{ column }}), 1, 1)                     AS first_char,
    substring(TRIM({{ column }}), -1, 1)                    AS last_char,
    length(regexp_replace({{ column }}, '[^{]', '')) - length(regexp_replace({{ column }}, '[^}]', '')) AS brace_imbalance,
    _metadata.file_path                                     AS data_file
FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE {{ column }} IS NOT NULL
  AND try_parse_json({{ column }}) IS NULL
ORDER BY char_length DESC
LIMIT 100
```

### Onset by data file and commit

```sql
SELECT
    _metadata.file_path                                     AS data_file,
    _metadata.file_modification_time                        AS written_at,
    COUNT(*)                                                AS rows_in_file,
    COUNT_IF({{ column }} IS NOT NULL AND try_parse_json({{ column }}) IS NULL) AS invalid_rows
FROM {{ catalog }}.{{ schema }}.{{ asset }}
GROUP BY _metadata.file_path, _metadata.file_modification_time
HAVING invalid_rows > 0
ORDER BY written_at DESC
LIMIT 100
```

With Change Data Feed enabled (`delta.enableChangeDataFeed = true`):

```sql
SELECT
    _commit_version,
    _commit_timestamp,
    COUNT_IF({{ column }} IS NOT NULL AND try_parse_json({{ column }}) IS NULL) AS invalid_rows,
    COUNT(*)                                                                   AS rows_changed
FROM table_changes('{{ catalog }}.{{ schema }}.{{ asset }}', {{ start_version }})
WHERE _change_type IN ('insert', 'update_postimage')
GROUP BY _commit_version, _commit_timestamp
HAVING invalid_rows > 0
ORDER BY _commit_version DESC
```

Then `DESCRIBE HISTORY {{ catalog }}.{{ schema }}.{{ asset }}` for those versions gives `operation`, `job` and `notebook`, which names the producer.

### Rescue-column detail (ingested tables)

For the rescue shape of the check, the `schema_conformity` diagnostic's rescued-fields query applies unchanged; for `_corrupt_record` tables:

```sql
SELECT
    substring(_corrupt_record, 1, 300)                      AS record_preview,
    length(_corrupt_record)                                 AS char_length,
    size(split(_corrupt_record, '{{ delimiter }}')) - 1     AS delimiter_count,
    COUNT(*) OVER ()                                        AS total_corrupt_rows
FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE _corrupt_record IS NOT NULL
LIMIT 100
```

`{{ delimiter }}` defaults to `,`. A `delimiter_count` that differs from the declared column count minus one is the classic unquoted-delimiter-in-a-field defect.
