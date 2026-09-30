WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, v
)
SELECT
    id,
    host,
    log_ts,
    latency_ms,
    status_code,
    k,
    v
FROM flattened
ORDER BY k NULLS LAST;
