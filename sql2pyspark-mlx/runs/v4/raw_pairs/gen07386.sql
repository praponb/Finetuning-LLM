WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, attr_value
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    COUNT(*) FILTER (WHERE key = 'region') AS region_rows,
    COUNT(*) FILTER (WHERE service = 'checkout') AS checkout_rows,
    MIN(log_ts) AS started_at,
    COUNT(*) FILTER (WHERE service = 'checkout') AS checkout_rows_2,
    COUNT(DISTINCT id) AS distinct_ids
FROM lines
GROUP BY DATE_TRUNC('DAY', log_ts)
HAVING COUNT(*) FILTER (WHERE key = 'region') > 1;
