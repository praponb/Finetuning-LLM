WITH enriched AS (
    SELECT src.*, b.stock_qty
    FROM order_items src
    JOIN products b
        ON src.product_id = b.id
)
SELECT
    product_id,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    PERCENTILE_APPROX(unit_price, ARRAY(0.25, 0.5, 0.75)) AS unit_price_quartiles,
    SUM(stock_qty) AS total_stock_qty
FROM enriched
GROUP BY product_id
ORDER BY distinct_order_ids DESC NULLS LAST;
