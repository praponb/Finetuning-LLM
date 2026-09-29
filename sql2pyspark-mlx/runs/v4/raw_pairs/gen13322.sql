WITH flattened_attrs AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        customer_id,
        created_at,
        items,
        ship_to,
        attributes,
        COALESCE(ship_to.city, 'UNKNOWN') AS city_or_unknown,
        ship_to.zip AS ship_zip,
        TRANSFORM(items, item -> item.product_id) AS product_ids
    FROM carts
)
-- Grouped metrics
SELECT
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    COUNT(*) AS cnt,
    MAX(created_at) AS last_created_at,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS span_minutes
FROM flattened_attrs
GROUP BY hour_window
HAVING cnt >= 1
ORDER BY span_minutes;
