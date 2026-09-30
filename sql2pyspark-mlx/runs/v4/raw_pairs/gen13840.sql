WITH flattened AS (
    SELECT
        id,
        service,
        host,
        latency_ms,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, attr_value
)
SELECT
    id,
    service,
    host,
    latency_ms,
    k,
    CASE WHEN k RLIKE '^(trace|region)$' THEN 'PRIORITY' ELSE 'CORE' END AS k_zone
FROM flattened
ORDER BY k NULLS FIRST, k_zone;
