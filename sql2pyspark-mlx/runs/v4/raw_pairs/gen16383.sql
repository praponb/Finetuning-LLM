SELECT
    id,
    service,
    host,
    log_ts,
    level,
    key,
    attr_value
FROM app_logs
LATERAL VIEW EXPLODE(context) li AS key, attr_value
