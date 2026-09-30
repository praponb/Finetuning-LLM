WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, value
)
SELECT
    id,
    service,
    level,
    latency_ms,
    status_code,
    k
FROM exploded
ORDER BY level DESC
