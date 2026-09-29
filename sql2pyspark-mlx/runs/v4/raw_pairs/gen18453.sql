WITH flattened AS (
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS k, attr_value
)
SELECT
    id,
    log_ts,
    latency_ms,
    k,
    attr_value
FROM flattened;
