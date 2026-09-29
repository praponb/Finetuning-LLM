WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, attr_value
),
parsed AS (
    -- Higher-order array functions
    SELECT
        log_ts,
        level,
        key,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS flag_bit2
    FROM flattened
    WHERE key <> 'version'
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        log_ts,
        level,
        key,
        status_code_high,
        LENGTH(level) AS level_length
    FROM parsed
    WHERE status_code_high <> 2
)
-- Grouped metrics
SELECT
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    key,
    CASE WHEN status_code_high >= 2 THEN 'HIGH' ELSE 'LOW' END AS status_code_high_tier,
    SIZE(COLLECT_SET(level)) AS num_levels,
    MAX(log_ts) AS last_log_ts,
    COUNT(*) FILTER (WHERE key = 'region') AS region_rows
FROM base
GROUP BY DATE_TRUNC('DAY', log_ts), key, CASE WHEN status_code_high >= 2 THEN 'HIGH' ELSE 'LOW' END
ORDER BY region_rows DESC
