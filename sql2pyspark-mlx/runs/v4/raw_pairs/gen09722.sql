WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        elem.product_id,
        elem.qty,
        elem.price
    FROM carts
    LATERAL VIEW EXPLODE(items) x AS elem
)
-- Grouped metrics
SELECT
    customer_id,
    CASE WHEN price >= 256 THEN 'PREMIUM' ELSE 'STANDARD' END AS price_tier,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    MIN(price) AS min_price,
    PERCENTILE_APPROX(price, ARRAY(0.25, 0.5, 0.75)) AS price_quartiles
FROM flattened
GROUP BY CUBE (customer_id, CASE WHEN price >= 256 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING distinct_customer_ids > 1
