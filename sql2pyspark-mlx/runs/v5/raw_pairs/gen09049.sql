WITH lines AS (
    SELECT
        id,
        service,
        host,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, value
)
SELECT
    id,
    service,
    host,
    key,
    value,
    CASE WHEN key RLIKE '^(trace|version)$' THEN 'CORE' WHEN key RLIKE '^(region)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS key_zone
FROM lines
ORDER BY id
