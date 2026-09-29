WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, value
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        service,
        value,
        CASE WHEN attr_key IN ('trace', 'version') THEN 'Core' ELSE 'Other' END AS attr_key_group,
        COALESCE(attr_key, 'OTHER') AS attr_key_filled
    FROM flattened
    WHERE attr_key IS NOT NULL
      AND host NOT IN ('h2', 'h4', 'h1')
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        attr_key_group,
        LENGTH(service) AS service_length
    FROM filtered
    WHERE service = 'checkout'
)
SELECT
    id,
    service,
    attr_key_group,
    service_length,
    CASE WHEN service RLIKE '^(auth|billing)$' THEN 'CORE' ELSE 'PRIORITY' END AS service_zone
FROM cleaned
ORDER BY id, service DESC
