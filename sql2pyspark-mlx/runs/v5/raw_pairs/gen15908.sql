WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, value
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    COUNT(*) AS num_rows,
    MIN(log_ts) AS started_at,
    SIZE(COLLECT_SET(attr_key)) AS num_attr_keys
FROM flattened
GROUP BY DATE_TRUNC('DAY', log_ts)
ORDER BY num_attr_keys NULLS FIRST;
