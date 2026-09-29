WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS k, attr_value
)
SELECT
    id,
    service,
    log_ts,
    status_code,
    attr_value
FROM flattened
ORDER BY id DESC, service;
