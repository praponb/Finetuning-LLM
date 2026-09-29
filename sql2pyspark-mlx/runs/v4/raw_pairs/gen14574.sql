SELECT
    order_id,
    CASE WHEN unit_price >= 400 THEN 'high' WHEN unit_price >= 130 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
    SUM(unit_price * quantity) AS revenue,
    COUNT(DISTINCT unit_price) AS unique_orders
FROM order_items
WHERE product_id IS NOT NULL OR unit_price >= 210
GROUP BY order_id, CASE WHEN unit_price >= 400 THEN 'high' WHEN unit_price >= 130 THEN 'mid' ELSE 'low' END
HAVING COUNT(quantity) >= 1;
