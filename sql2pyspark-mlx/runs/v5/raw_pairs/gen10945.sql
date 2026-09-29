SELECT
    id,
    service,
    host,
    log_ts,
    level,
    status_code,
    attr_key,
    attr_value
FROM app_logs
LATERAL VIEW EXPLODE(context) li AS attr_key, attr_value
