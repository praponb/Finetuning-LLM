WITH exploded AS (
    SELECT
        id,
        service,
        latency_ms,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS key, value
)
SELECT
    key,
    COUNT(*) FILTER (WHERE key = 'trace') AS trace_rows,
    SUM(latency_ms) FILTER (WHERE key = 'region') AS region_total_latency_ms,
    SUM(latency_ms) FILTER (WHERE service = 'checkout') AS checkout_total_latency_ms
FROM exploded
GROUP BY 1
ORDER BY region_total_latency_ms DESC NULLS FIRST
