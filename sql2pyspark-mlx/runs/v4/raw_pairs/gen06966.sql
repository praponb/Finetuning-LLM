WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, value
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    CASE WHEN status_code >= 411 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_tier,
    SUM(latency_ms) FILTER (WHERE service = 'checkout') AS checkout_total_latency_ms,
    COUNT_IF(service = 'checkout') AS checkout_events,
    SUM(latency_ms) FILTER (WHERE key = 'trace') AS trace_total_latency_ms
FROM flattened
GROUP BY hour_window, status_code_tier
ORDER BY status_code_tier DESC NULLS FIRST;
