SELECT
    id,
    service,
    level,
    latency_ms,
    key,
    attr_value
FROM app_logs
LATERAL VIEW EXPLODE(context) li AS key, attr_value
