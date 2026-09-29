WITH lines AS (
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, value
)
SELECT
    service,
    status_code,
    k,
    value,
    CASE WHEN service RLIKE '^(billing|search)$' THEN 'EXTENDED' ELSE 'CORE' END AS service_zone
FROM lines;
