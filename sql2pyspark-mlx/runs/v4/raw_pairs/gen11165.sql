WITH flattened AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, value
)
SELECT
    id,
    level,
    status_code,
    key,
    value,
    CASE WHEN key RLIKE '^(trace|version)$' THEN 'GROUP_A' ELSE 'CORE' END AS key_zone
FROM flattened
