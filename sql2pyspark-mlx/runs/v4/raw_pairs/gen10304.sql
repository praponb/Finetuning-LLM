SELECT
    order_id,
    CASE WHEN quantity >= 6 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END AS quantity_bucket,
    SUM(unit_price) AS total_unit_price,
    SUM(quantity) AS total_quantity,
    COUNT(DISTINCT quantity) AS unique_orders
FROM order_items
GROUP BY order_id, CASE WHEN quantity >= 6 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END
ORDER BY unique_orders DESC;
