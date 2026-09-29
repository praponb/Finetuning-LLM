WITH flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        created_at,
        coupon_codes,
        items,
        attributes,
        ARRAY_CONTAINS(MAP_KEYS(attributes), 'utm_source') AS has_utm_source,
        EXISTS(items, x -> x.price > 100) AS has_premium_item,
        SIZE(items) AS num_items,
        ship_to.country AS ship_country,
        COALESCE(attributes['utm_source'], 'none') AS utm_source_or_default
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'WELCOME5')
)
-- Grouped metrics
SELECT
    CASE WHEN num_items >= 2 THEN 'LARGE' ELSE 'SMALL' END AS num_items_tier,
    ship_country,
    PERCENTILE_APPROX(num_items, 0.5) AS num_items_p50,
    MAX(created_at) AS last_created_at,
    MIN(created_at) AS first_created_at
FROM flattened_attrs
GROUP BY CASE WHEN num_items >= 2 THEN 'LARGE' ELSE 'SMALL' END, ship_country
HAVING num_items_p50 > 0
