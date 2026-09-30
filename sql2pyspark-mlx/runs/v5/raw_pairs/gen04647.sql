SELECT order_id, MAX(unit_price) - MIN(unit_price) unit_price_range, COUNT(*) order_item_count
FROM order_items
WHERE order_id = 10 AND (unit_price BETWEEN 120 AND 190 OR unit_price > 360)
GROUP BY order_id
HAVING unit_price_range >= 60;
