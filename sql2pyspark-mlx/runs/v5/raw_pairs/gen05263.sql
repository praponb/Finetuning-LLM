WITH flattened AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS key, attr_value
)
SELECT
    id,
    host,
    latency_ms,
    key,
    attr_value
FROM flattened
ORDER BY host DESC NULLS FIRST, key;
