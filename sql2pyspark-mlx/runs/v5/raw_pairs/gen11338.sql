WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, value
)
SELECT
    id,
    log_ts,
    latency_ms,
    status_code,
    k,
    value,
    CASE WHEN k RLIKE '^(version|trace)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS k_zone
FROM exploded;
