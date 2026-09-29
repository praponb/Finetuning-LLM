WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, v
)
SELECT
    id,
    service,
    v
FROM lines
ORDER BY service DESC NULLS FIRST
