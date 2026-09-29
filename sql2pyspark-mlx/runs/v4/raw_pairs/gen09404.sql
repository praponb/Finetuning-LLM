WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, value
)
SELECT
    id,
    host,
    log_ts,
    level,
    status_code,
    attr_key,
    value,
    CASE WHEN attr_key RLIKE '^(version|trace)$' THEN 'CORE' ELSE 'GROUP_A' END AS attr_key_zone
FROM flattened
ORDER BY attr_key NULLS FIRST;
