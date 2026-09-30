WITH lines AS (
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, value
)
SELECT
    service,
    log_ts,
    latency_ms,
    value
FROM lines
ORDER BY service DESC
