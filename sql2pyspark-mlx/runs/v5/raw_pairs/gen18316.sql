WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS k, v
)
SELECT
    id,
    service,
    host,
    log_ts,
    level,
    k,
    v,
    CASE WHEN level RLIKE '^(WARN|DEBUG)$' THEN 'PRIORITY' WHEN level RLIKE '^(INFO|ERROR)$' THEN 'EXTENDED' ELSE 'CORE' END AS level_zone
FROM lines
ORDER BY level_zone DESC;
