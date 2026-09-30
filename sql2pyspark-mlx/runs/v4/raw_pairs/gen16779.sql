SELECT
    id,
    service,
    host,
    k,
    attr_value
FROM app_logs
LATERAL VIEW OUTER EXPLODE(context) e AS k, attr_value
