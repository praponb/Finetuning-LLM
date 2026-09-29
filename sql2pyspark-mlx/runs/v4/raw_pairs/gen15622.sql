WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        ROUND(unit_price * 1.07, 2) AS unit_price_scaled
    FROM order_items
    WHERE product_id IS NOT NULL
)
-- Multi-level aggregation
SELECT
    CASE WHEN unit_price_scaled >= 68 THEN 'LARGE' ELSE 'SMALL' END AS unit_price_scaled_tier,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    PERCENTILE_APPROX(unit_price, 0.9) AS unit_price_p90,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    COUNT(*) AS num_rows
FROM cleaned
GROUP BY CUBE (CASE WHEN unit_price_scaled >= 68 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY unit_price_p90
