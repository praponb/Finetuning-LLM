WITH exploded AS (
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS k, attr_value
)
SELECT
    CASE WHEN status_code >= 459 THEN 'HIGH' ELSE 'LOW' END AS status_code_tier,
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    host,
    COUNT_IF(k = 'trace') AS trace_events,
    ROUND(SUM(status_code) / NULLIF(COUNT(CASE WHEN host = 'h2' THEN 1 END), 0), 2) AS status_code_per_h2
FROM exploded
GROUP BY CASE WHEN status_code >= 459 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('DAY', log_ts), host
ORDER BY trace_events NULLS LAST
