SELECT
    id,
    host,
    level,
    status_code,
    k,
    v
FROM app_logs
LATERAL VIEW EXPLODE(context) li AS k, v
