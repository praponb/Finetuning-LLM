WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, attr_value
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN status_code >= 468 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_tier,
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    MIN(log_ts) AS started_at,
    SIZE(COLLECT_SET(service)) AS num_services,
    MIN(latency_ms) AS min_latency_ms
FROM lines
GROUP BY 1, 2;
