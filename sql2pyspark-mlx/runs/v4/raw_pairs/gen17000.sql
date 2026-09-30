WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, v
)
-- Grouped metrics
SELECT
    service,
    COUNT(DISTINCT service) AS distinct_services,
    MIN(status_code) AS min_status_code
FROM exploded
GROUP BY service;
