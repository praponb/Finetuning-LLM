WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, v
)
SELECT
    id,
    service,
    host,
    level,
    attr_key,
    v,
    CASE WHEN attr_key RLIKE '^(region|version)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS attr_key_zone
FROM lines
ORDER BY attr_key NULLS LAST
