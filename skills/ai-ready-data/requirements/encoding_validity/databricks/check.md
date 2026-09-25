# Check: encoding_validity

Fraction of non-null values in a text column that are free of encoding damage: invalid UTF-8, the Unicode replacement character, and stray control characters.

## Context

Column-scoped data scan. A value is invalid when any of these holds:

- `is_valid_utf8(col)` is false: the bytes are not well-formed UTF-8. Delta stores strings as UTF-8 bytes and Spark does not validate them on write, so a file ingested as Latin-1 or Windows-1252 and declared UTF-8 lands here.
- It contains U+FFFD (`chr(65533)`), the replacement character a decoder writes when it gives up. The value is already lossy.
- It contains a C0 control character other than TAB, LF and CR (`[\x00-\x08\x0B\x0C\x0E-\x1F]`), which almost always means a binary blob or a terminal escape got loaded as text.

`value = valid_rows / non_null_rows`. NULLs are excluded; `data_completeness` covers them.

Strength is **data**. `is_valid_utf8()` needs DBR 16.1 or later (all current SQL warehouses have it). On older clusters use the fallback variant, which cannot detect invalid byte sequences directly and instead looks for U+FFFD, control characters and common mojibake sequences (`Ã©`, `â€™`, `Â `), the fingerprints of UTF-8 text decoded as Latin-1. Mojibake is technically valid UTF-8, so the primary variant does not flag it either; the diagnostic counts it separately because the fix is different (re-decode, not strip).

Placeholders: `{{ sample_rows }}`, default 1,000,000.

Regex literals use doubled backslashes because Databricks SQL processes escape sequences in string literals before the pattern reaches the regex engine.

Returns NULL when the column has no non-null values.

## SQL

### UTF-8 validity, replacement character, control characters (primary)

```sql
WITH col_check AS (
    SELECT
        COUNT(*)                                                              AS non_null_rows,
        COUNT_IF(
            is_valid_utf8({{ column }})
            AND NOT contains({{ column }}, chr(65533))
            AND NOT REGEXP_LIKE({{ column }}, '[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F]')
        )                                                                     AS valid_rows
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
    WHERE {{ column }} IS NOT NULL
)
SELECT
    '{{ asset }}'                                       AS table_name,
    '{{ column }}'                                      AS column_name,
    valid_rows,
    non_null_rows,
    valid_rows::DOUBLE / NULLIF(non_null_rows, 0)       AS value
FROM col_check
```

### Fallback without is_valid_utf8 (variant)

For DBR below 16.1. Adds a mojibake pattern so double-encoded text is caught, since invalid bytes cannot be tested directly.

```sql
WITH col_check AS (
    SELECT
        COUNT(*)                                                              AS non_null_rows,
        COUNT_IF(
            NOT contains({{ column }}, chr(65533))
            AND NOT REGEXP_LIKE({{ column }}, '[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F]')
            AND NOT REGEXP_LIKE({{ column }}, '(Ã[\\x80-\\xBF]|â€[\\x80-\\xBF™œž]|Â[\\xA0-\\xBF])')
        )                                                                     AS valid_rows
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
    WHERE {{ column }} IS NOT NULL
)
SELECT
    '{{ asset }}'                                       AS table_name,
    '{{ column }}'                                      AS column_name,
    valid_rows,
    non_null_rows,
    valid_rows::DOUBLE / NULLIF(non_null_rows, 0)       AS value
FROM col_check
```

The mojibake pattern has a small false-positive rate on legitimate Portuguese or French text containing `Ã` followed by a vowel (`João` is fine, `Ã©` is not); if the column is in such a language, drop that line.

### Sampled (variant)

```sql
WITH col_check AS (
    SELECT
        COUNT(*)                                                              AS non_null_rows,
        COUNT_IF(
            is_valid_utf8({{ column }})
            AND NOT contains({{ column }}, chr(65533))
            AND NOT REGEXP_LIKE({{ column }}, '[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F]')
        )                                                                     AS valid_rows
    FROM {{ catalog }}.{{ schema }}.{{ asset }} TABLESAMPLE ({{ sample_rows }} ROWS)
    WHERE {{ column }} IS NOT NULL
)
SELECT
    '{{ asset }}'                                       AS table_name,
    '{{ column }}'                                      AS column_name,
    valid_rows,
    non_null_rows,
    valid_rows::DOUBLE / NULLIF(non_null_rows, 0)       AS value
FROM col_check
```

Encoding damage clusters by load (one bad file, one bad producer version), and `TABLESAMPLE (n ROWS)` reads the oldest files first, so the sample can miss a recent bad load entirely. Use it for triage only.
