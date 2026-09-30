SELECT COUNT(DISTINCT quantity) AS unique_orders, ROUND(AVG(unit_price), 2) AS avg_unit_price
FROM order_items
WHERE order_id IN (22, 30, 32)
