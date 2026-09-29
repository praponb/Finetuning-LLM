SELECT
    id,
    service,
    level,
    status_code,
    k,
    value
FROM app_logs
LATERAL VIEW EXPLODE(context) e AS k, value
