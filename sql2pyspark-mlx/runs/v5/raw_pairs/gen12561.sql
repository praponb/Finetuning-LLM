SELECT
    id,
    status_code,
    key,
    value
FROM app_logs
LATERAL VIEW EXPLODE(context) x AS key, value;
