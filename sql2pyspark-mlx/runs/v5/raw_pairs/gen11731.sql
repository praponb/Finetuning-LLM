SELECT
    id,
    host,
    log_ts,
    level,
    status_code,
    k,
    attr_value
FROM app_logs
LATERAL VIEW OUTER EXPLODE(context) e AS k, attr_value;
