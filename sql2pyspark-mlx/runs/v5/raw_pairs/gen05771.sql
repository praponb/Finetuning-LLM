WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, v
),
unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        log_ts,
        key,
        v,
        CAST((status_code & 2) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(has_fault, 1) AS has_fault_rounded
    FROM unpacked
    WHERE key IS NOT NULL
),
valid_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        id,
        v,
        has_fault,
        status_code_high,
        DATE_TRUNC('year', log_ts) AS log_ts_period
    FROM valid
    WHERE log_ts > TIMESTAMP '2025-06-01 09:00:00'
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM valid_app_logs
    EXCEPT ALL
    SELECT id
    FROM app_logs
    WHERE latency_ms > 1005
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid_app_logs s
    JOIN kept_keys k ON s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN status_code_high >= 3 THEN 'HIGH' ELSE 'LOW' END AS status_code_high_tier,
    COUNT(*) AS cnt,
    COUNT(DISTINCT id) AS distinct_ids,
    (UNIX_TIMESTAMP(MAX(log_ts_period)) - UNIX_TIMESTAMP(MIN(log_ts_period))) / 60 AS duration_min,
    COUNT(DISTINCT id) AS distinct_ids_2
FROM verified
GROUP BY CASE WHEN status_code_high >= 3 THEN 'HIGH' ELSE 'LOW' END
ORDER BY distinct_ids_2 NULLS LAST
