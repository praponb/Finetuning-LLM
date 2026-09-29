WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, v
)
SELECT
    id,
    service,
    host,
    status_code,
    key,
    CASE WHEN service RLIKE '^(checkout|billing)$' THEN 'PRIORITY' WHEN service RLIKE '^(auth|search)$' THEN 'EXTENDED' ELSE 'CORE' END AS service_zone
FROM exploded
ORDER BY host, status_code;
