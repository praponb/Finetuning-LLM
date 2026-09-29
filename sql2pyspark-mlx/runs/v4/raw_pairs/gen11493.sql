WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, value
)
SELECT
    id,
    latency_ms,
    status_code,
    key,
    value,
    CASE WHEN key RLIKE '^(region|trace)$' THEN 'GROUP_B' WHEN key RLIKE '^(version)$' THEN 'CORE' ELSE 'EXTENDED' END AS key_zone
FROM lines
ORDER BY status_code, key_zone;
