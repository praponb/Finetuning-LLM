SELECT
    id,
    log_ts,
    latency_ms,
    status_code,
    key,
    v
FROM app_logs
LATERAL VIEW OUTER EXPLODE(context) e AS key, v;
