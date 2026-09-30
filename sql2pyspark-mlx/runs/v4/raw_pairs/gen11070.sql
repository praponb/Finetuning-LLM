SELECT
    id,
    service,
    log_ts,
    latency_ms,
    attr_key,
    v
FROM app_logs
LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, v;
