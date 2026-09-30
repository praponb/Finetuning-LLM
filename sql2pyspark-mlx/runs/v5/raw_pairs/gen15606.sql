WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, attr_value
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    COUNT(*) AS num_rows,
    MAX(log_ts) AS ended_at,
    MAX(log_ts) AS last_log_ts
FROM flattened
GROUP BY DATE_TRUNC('DAY', log_ts), DATE_TRUNC('HOUR', log_ts)
ORDER BY activity_day NULLS FIRST;
