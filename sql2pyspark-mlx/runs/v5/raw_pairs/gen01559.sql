WITH flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        coupon_codes,
        ship_to,
        attributes,
        ARRAY_CONTAINS(MAP_KEYS(attributes), 'device') AS has_device,
        attributes['device'] AS device,
        TRANSFORM(items, i -> i.product_id) AS product_ids,
        AGGREGATE(items, 0D, (acc, i) -> acc + i.qty * i.price, acc -> ROUND(acc, 2)) AS items_total
    FROM carts
)
-- Grouped metrics
SELECT
    CASE WHEN items_total >= 1630 THEN 'LARGE' ELSE 'SMALL' END AS items_total_tier,
    COUNT(*) AS cnt,
    APPROX_PERCENTILE(items_total, 0.5) AS items_total_p50,
    PERCENTILE_APPROX(items_total, 0.9) AS items_total_p90
FROM flattened_attrs
GROUP BY CASE WHEN items_total >= 1630 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY items_total_p50;
