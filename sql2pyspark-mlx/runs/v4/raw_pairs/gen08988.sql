WITH flattened AS (
    SELECT
        id,
        service,
        host,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, attr_value
)
SELECT
    id,
    host,
    k,
    attr_value,
    CASE WHEN k RLIKE '^(version|region)$' THEN 'GROUP_A' WHEN k RLIKE '^(trace)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS k_zone
FROM flattened
ORDER BY id DESC, k DESC
