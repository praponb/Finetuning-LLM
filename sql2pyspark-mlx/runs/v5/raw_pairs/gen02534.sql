WITH exploded AS (
    SELECT
        id,
        host,
        log_ts,
        level,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, value
)
SELECT
    id,
    host,
    level,
    status_code,
    key,
    value,
    CASE WHEN key RLIKE '^(trace|region)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS key_zone
FROM exploded
ORDER BY key_zone DESC NULLS LAST, host
