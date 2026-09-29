WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, attr_value
)
SELECT
    host,
    latency_ms,
    status_code,
    attr_key,
    attr_value
FROM lines
ORDER BY status_code DESC, attr_key DESC;
