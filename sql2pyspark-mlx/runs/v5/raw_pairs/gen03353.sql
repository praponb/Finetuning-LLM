WITH joined AS (
    -- Attach products attributes
    SELECT
        a.id,
        a.store_id,
        a.product_id,
        a.quantity,
        p.price,
        p.category,
        p.stock_qty
    FROM inventory a
    JOIN products p
        ON a.product_id = p.id
    WHERE LENGTH(p.product_name) > 12
)
-- Grouped metrics
SELECT
    product_id,
    CASE WHEN price >= 990 THEN 'HIGH' ELSE 'LOW' END AS price_tier,
    APPROX_PERCENTILE(price, ARRAY(0.25, 0.5, 0.75)) AS price_quartiles,
    BIT_OR(quantity) AS quantity_bit_or,
    COUNT(CASE WHEN category = 'Grocery' THEN 1 END) AS grocery_count,
    GROUPING_ID() AS grouping_id
FROM joined
GROUP BY GROUPING SETS ((product_id, CASE WHEN price >= 990 THEN 'HIGH' ELSE 'LOW' END), (product_id), ())
ORDER BY quantity_bit_or DESC NULLS FIRST
