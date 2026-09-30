WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, value
)
-- Running and moving aggregates
SELECT
    *,
    CORR(latency_ms, status_code) OVER (PARTITION BY key) corr_latency_ms_status_code,
    LEAD(latency_ms, 1) OVER (PARTITION BY service ORDER BY status_code, id, key) next_latency_ms,
    SUM(latency_ms) OVER (PARTITION BY host, key ORDER BY latency_ms, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_latency_ms
FROM exploded;
