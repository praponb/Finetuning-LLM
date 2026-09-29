WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, v
)
SELECT id, host
FROM exploded
ORDER BY host DESC;
