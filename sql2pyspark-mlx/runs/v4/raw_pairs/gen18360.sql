WITH parsed AS (
    SELECT
        id,
        customer_id,
        created_at,
        ship_to,
        attributes,
        ship_to.country AS ship_country,
        COALESCE(attributes['gift'], 'none') AS gift_or_default,
        ship_to.city AS ship_city,
        EXISTS(items, li -> li.price > 100) AS has_premium_item
    FROM carts
)
SELECT
    customer_id,
    DATE_TRUNC('DAY', created_at) AS activity_day,
    ship_country,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS span_minutes,
    COUNT(*) AS row_count,
    SIZE(COLLECT_SET(ship_city)) AS num_ship_cities
FROM parsed
GROUP BY ROLLUP (customer_id, DATE_TRUNC('DAY', created_at), ship_country)
HAVING row_count < 1
ORDER BY customer_id DESC;
