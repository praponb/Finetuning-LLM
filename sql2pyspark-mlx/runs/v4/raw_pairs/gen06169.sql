WITH parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        customer_id,
        items,
        ship_to,
        AGGREGATE(items, 0.0D, (acc, item) -> acc + item.qty * item.price, acc -> ROUND(acc, 2)) AS items_total,
        ship_to.city AS ship_city,
        ARRAY_JOIN(ARRAY_SORT(coupon_codes), ',') AS coupons_text,
        ARRAY_CONTAINS(MAP_KEYS(attributes), 'weight_kg') AS has_weight_kg,
        EXISTS(items, item -> item.price > 100) AS has_premium_item
    FROM carts
    WHERE customer_id IS NULL
)
-- Grouped metrics
SELECT
    ship_city,
    CASE WHEN items_total >= 1410 THEN 'LARGE' ELSE 'SMALL' END AS items_total_tier,
    COUNT(DISTINCT ship_city) AS distinct_ship_cities,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    COUNT(CASE WHEN ship_city = 'Tokyo' THEN 1 END) AS tokyo_count,
    AVG(items_total) AS avg_items_total,
    APPROX_PERCENTILE(items_total, ARRAY(0.25, 0.5, 0.75)) AS items_total_quartiles
FROM parsed
GROUP BY ROLLUP (ship_city, CASE WHEN items_total >= 1410 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY distinct_ship_cities NULLS FIRST
