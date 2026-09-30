SELECT
    id,
    service,
    host,
    log_ts,
    level,
    latency_ms,
    key,
    attr_value
FROM app_logs
LATERAL VIEW OUTER EXPLODE(context) t AS key, attr_value
