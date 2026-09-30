WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, v
)
SELECT
    id,
    host,
    v
FROM exploded
ORDER BY id DESC NULLS LAST, host
