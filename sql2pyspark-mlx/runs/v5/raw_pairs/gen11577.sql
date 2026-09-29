WITH flattened AS (
    SELECT
        id,
        service,
        host,
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
    host,
    log_ts,
    attr_key
FROM flattened
ORDER BY service NULLS LAST, log_ts DESC;
