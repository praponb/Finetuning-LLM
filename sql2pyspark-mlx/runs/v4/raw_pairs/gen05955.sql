SELECT
    id,
    host,
    log_ts,
    level,
    status_code,
    k,
    attr_value
FROM app_logs
LATERAL VIEW EXPLODE(context) t AS k, attr_value
