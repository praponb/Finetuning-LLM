WITH flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        customer_id,
        created_at,
        items,
        ship_to,
        TRANSFORM(
            FILTER(items, i -> i.promo IS NOT NULL),
            i -> STRUCT(i.product_id AS pid, ROUND(i.price * 0.9, 2) AS discounted_price)
        ) AS promo_lines,
        ARRAY_CONTAINS(MAP_KEYS(attributes), 'gift') AS has_gift,
        COALESCE(attributes['gift'], 'unknown') AS gift_or_default,
        TRANSFORM(items, i -> i.product_id) AS product_ids
    FROM carts
    WHERE customer_id IS NOT NULL
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('DAY', created_at) AS activity_day,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min,
    MIN(created_at) AS first_seen,
    MAX(created_at) AS last_seen,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min_2,
    MAX(created_at) AS last_created_at
FROM flattened_attrs
GROUP BY DATE_TRUNC('DAY', created_at)
HAVING duration_min_2 > 0
ORDER BY duration_min NULLS LAST
