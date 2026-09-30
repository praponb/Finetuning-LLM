SELECT
    id,
    service,
    host,
    level,
    latency_ms,
    status_code,
    attr_key,
    v
FROM app_logs
LATERAL VIEW EXPLODE(context) e AS attr_key, v
