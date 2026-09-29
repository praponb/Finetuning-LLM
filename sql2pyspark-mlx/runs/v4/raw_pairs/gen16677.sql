WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, v
)
SELECT
    id,
    host,
    log_ts,
    level,
    latency_ms,
    k,
    CASE WHEN k RLIKE '^(region|trace)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS k_zone
FROM exploded
ORDER BY k DESC;
