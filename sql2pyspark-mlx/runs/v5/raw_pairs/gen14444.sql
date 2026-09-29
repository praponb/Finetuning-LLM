WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, v
)
SELECT
    host,
    level,
    status_code,
    key,
    v
FROM exploded
ORDER BY host DESC NULLS FIRST, status_code
