WITH flattened AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, value
)
SELECT
    id,
    host,
    log_ts,
    k,
    value,
    CASE WHEN host RLIKE '^(h4|h1)$' THEN 'GROUP_A' ELSE 'CORE' END AS host_zone
FROM flattened
ORDER BY log_ts DESC NULLS LAST;
