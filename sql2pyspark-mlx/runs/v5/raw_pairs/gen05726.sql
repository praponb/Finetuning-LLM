SELECT
    CASE WHEN unit_price >= 320 THEN 'high' WHEN unit_price >= 90 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
    product_id,
    COUNT(DISTINCT quantity) AS unique_orders,
    SUM(quantity) AS total_quantity
FROM order_items
WHERE quantity <= 3 AND product_id IS NOT NULL
GROUP BY 1, 2
