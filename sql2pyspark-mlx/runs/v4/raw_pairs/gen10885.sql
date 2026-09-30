WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, value
),
parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        k,
        CAST((status_code & 1) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM parsed
    INTERSECT ALL
    SELECT id
    FROM app_logs
    WHERE level = 'INFO'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM parsed s
    INNER JOIN remaining_keys k ON s.id = k.id
)
SELECT
    host,
    status_code,
    k,
    is_flagged,
    status_code_high,
    CASE WHEN k RLIKE '^(region|trace)$' THEN 'GROUP_A' WHEN k RLIKE '^(version)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS k_zone
FROM kept_rows
ORDER BY is_flagged ASC NULLS LAST, k_zone ASC;
