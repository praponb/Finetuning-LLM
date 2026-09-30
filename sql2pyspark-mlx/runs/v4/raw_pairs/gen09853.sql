WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, value
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    MAX(status_code) FILTER (WHERE service = 'search') AS search_max_status_code,
    MIN(log_ts) AS first_log_ts,
    COUNT(CASE WHEN service = 'search' THEN 1 END) AS search_count,
    AVG(status_code) AS avg_status_code,
    ROUND(SUM(status_code) / NULLIF(COUNT(CASE WHEN service = 'search' THEN 1 END), 0), 2) AS status_code_per_search
FROM lines
GROUP BY activity_day
ORDER BY search_max_status_code DESC
