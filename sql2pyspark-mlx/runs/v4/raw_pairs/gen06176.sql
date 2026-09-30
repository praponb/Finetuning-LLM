SELECT
    id,
    host,
    latency_ms,
    status_code,
    attr_key,
    v
FROM app_logs
LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, v;
