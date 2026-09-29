WITH exploded AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, v
)
SELECT
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    SIZE(COLLECT_SET(host)) AS num_hosts,
    COUNT(*) AS num_rows,
    APPROX_PERCENTILE(latency_ms, 0.95) AS latency_ms_p95,
    COUNT(*) FILTER (WHERE service = 'auth') AS auth_rows,
    PERCENTILE_APPROX(latency_ms, ARRAY(0.25, 0.5, 0.75)) AS latency_ms_quartiles
FROM exploded
GROUP BY DATE_TRUNC('DAY', log_ts)
HAVING auth_rows > 1
ORDER BY latency_ms_p95
