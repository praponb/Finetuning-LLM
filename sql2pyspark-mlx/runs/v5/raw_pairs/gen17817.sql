WITH parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        customer_id,
        created_at,
        attributes,
        COALESCE(attributes['utm_source'], 'unknown') AS utm_source_or_default,
        ship_to.zip AS ship_zip,
        ship_to.country AS ship_country,
        EXISTS(items, item -> item.price > 100) AS has_premium_item,
        AGGREGATE(items, CAST(0 AS DOUBLE), (acc, item) -> acc + item.qty * item.price, acc -> ROUND(acc, 2)) AS items_total
    FROM carts
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    COUNT_IF(ship_country = 'US') AS us_events,
    SORT_ARRAY(COLLECT_SET(ship_country)) AS ship_country_list,
    SUM(items_total) AS total_items_total,
    MIN(created_at) AS first_created_at,
    MAX(created_at) AS last_created_at
FROM parsed
GROUP BY DATE_TRUNC('HOUR', created_at)
ORDER BY total_items_total DESC;
