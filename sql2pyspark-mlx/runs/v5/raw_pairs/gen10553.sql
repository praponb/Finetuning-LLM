WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, attr_value
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM lines
    EXCEPT ALL
    SELECT id
    FROM app_logs
    WHERE latency_ms != 608
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM lines s
    JOIN kept_keys k ON s.id = k.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN attr_key = 'trace' THEN 1 ELSE 0 END AS is_trace,
        COALESCE(attr_key, 'N/A') AS attr_key_filled
    FROM kept_rows
    WHERE log_ts >= TIMESTAMP '2025-06-01 08:00:00'
),
filtered_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        *,
        WEEKOFYEAR(log_ts) AS log_ts_weekofyear,
        LENGTH(attr_key) AS attr_key_length,
        COALESCE(attr_key, 'N/A') AS resolved_attr_key
    FROM filtered
    WHERE HOUR(log_ts) BETWEEN 6 AND 9
)
SELECT
    id,
    log_ts,
    attr_key,
    attr_value,
    is_trace,
    attr_key_filled,
    log_ts_weekofyear,
    attr_key_length,
    resolved_attr_key,
    CASE WHEN attr_key RLIKE '^(region|version)$' THEN 'GROUP_A' WHEN attr_key RLIKE '^(trace)$' THEN 'CORE' ELSE 'GROUP_B' END AS attr_key_zone
FROM filtered_app_logs
ORDER BY is_trace DESC
