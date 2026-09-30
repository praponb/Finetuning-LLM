WITH flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        customer_id,
        coupon_codes,
        items,
        ship_to,
        attributes,
        ARRAY_MAX(TRANSFORM(items, i -> i.price)) AS max_item_price,
        SIZE(FILTER(items, i -> i.qty >= 3)) AS bulk_lines,
        COALESCE(ship_to.country, 'UNKNOWN') AS country_or_unknown,
        ship_to.zip AS ship_zip,
        ARRAY_CONTAINS(coupon_codes, 'SPRING10') AS used_spring10
    FROM carts
)
-- Multi-level aggregation
SELECT
    CASE WHEN bulk_lines >= 3 THEN 'LARGE' ELSE 'SMALL' END AS bulk_lines_tier,
    customer_id,
    APPROX_PERCENTILE(bulk_lines, 0.9) AS bulk_lines_p90,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    PERCENTILE_APPROX(bulk_lines, 0.5) AS bulk_lines_p50,
    APPROX_PERCENTILE(max_item_price, 0.95) AS max_item_price_p95,
    PERCENTILE_APPROX(max_item_price, 0.9) AS max_item_price_p90,
    MIN(max_item_price) AS min_max_item_price
FROM flattened_attrs
GROUP BY CASE WHEN bulk_lines >= 3 THEN 'LARGE' ELSE 'SMALL' END, customer_id
HAVING bulk_lines_p50 >= 0
ORDER BY bulk_lines_p50
