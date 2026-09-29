WITH exploded AS (
    SELECT
        id,
        log_ts,
        level,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, value
)
SELECT
    id,
    log_ts,
    status_code,
    key,
    value
FROM exploded
ORDER BY log_ts DESC, status_code DESC
