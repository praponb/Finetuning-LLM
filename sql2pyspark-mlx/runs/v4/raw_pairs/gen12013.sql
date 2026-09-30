WITH exploded AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, attr_value
)
SELECT
    id,
    service,
    status_code,
    key,
    attr_value,
    CASE WHEN service RLIKE '^(billing|auth)$' THEN 'EXTENDED' WHEN service RLIKE '^(search|checkout)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS service_zone
FROM exploded
ORDER BY service NULLS FIRST, key;
