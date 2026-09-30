WITH flattened AS (
    SELECT
        id,
        service,
        latency_ms,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, v
)
SELECT
    id,
    service,
    latency_ms,
    attr_key,
    v,
    CASE WHEN attr_key RLIKE '^(region|trace)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS attr_key_zone
FROM flattened
ORDER BY attr_key DESC NULLS LAST, service;
