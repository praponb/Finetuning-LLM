SELECT
    id,
    service,
    log_ts,
    attr_key,
    value
FROM app_logs
LATERAL VIEW EXPLODE(context) t AS attr_key, value
