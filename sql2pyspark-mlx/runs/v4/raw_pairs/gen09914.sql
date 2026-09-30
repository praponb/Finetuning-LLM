WITH exploded AS (
    SELECT
        id,
        host,
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
    log_ts,
    level,
    latency_ms,
    k
FROM exploded
ORDER BY id NULLS FIRST;
