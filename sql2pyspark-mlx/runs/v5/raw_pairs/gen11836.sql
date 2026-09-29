WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        level,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, attr_value
),
parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        log_ts,
        k,
        CAST((status_code & 1) != 0 AS INT) AS flag_bit1,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM parsed
    INTERSECT ALL
    SELECT id
    FROM app_logs
    WHERE TO_DATE(log_ts) >= '2024-09-30'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM parsed s
    INNER JOIN matched_keys k ON s.id = k.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        k,
        flag_bit1,
        status_code_high,
        flag_bit1 - status_code_high AS flag_bit1_minus_status_code_high,
        flag_bit1 - status_code_high AS flag_bit1_minus_status_code_high_2,
        COALESCE(k, 'DIRECT') AS k_filled
    FROM verified
    WHERE k IS NOT NULL
)
-- Multi-level aggregation
SELECT
    CASE WHEN flag_bit1 >= 1 THEN 'HIGH' ELSE 'LOW' END AS flag_bit1_tier,
    k,
    MIN(flag_bit1_minus_status_code_high_2) AS min_flag_bit1_minus_status_code_high_2,
    COUNT_IF(k = 'version') AS version_events,
    COUNT(DISTINCT k) AS distinct_ks
FROM base
GROUP BY CUBE (CASE WHEN flag_bit1 >= 1 THEN 'HIGH' ELSE 'LOW' END, k)
ORDER BY min_flag_bit1_minus_status_code_high_2 DESC NULLS LAST;
