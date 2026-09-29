SELECT
    id,
    service,
    log_ts,
    level,
    status_code,
    key,
    v
FROM app_logs
LATERAL VIEW OUTER EXPLODE(context) t AS key, v;
