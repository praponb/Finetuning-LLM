SELECT
    CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 5 THEN 'mid' ELSE 'low' END AS quantity_bucket,
    order_id,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    COUNT(DISTINCT quantity) AS unique_orders,
    MAX(quantity) AS max_quantity
FROM order_items
GROUP BY quantity_bucket, order_id
HAVING ROUND(AVG(quantity), 2) >= 78;
