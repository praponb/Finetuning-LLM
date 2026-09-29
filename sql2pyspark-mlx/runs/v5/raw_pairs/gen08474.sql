WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, v
)
SELECT
    id,
    service,
    log_ts,
    attr_key,
    CASE WHEN service RLIKE '^(search|checkout)$' THEN 'PRIORITY' WHEN service RLIKE '^(auth|billing)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS service_zone
FROM exploded;
