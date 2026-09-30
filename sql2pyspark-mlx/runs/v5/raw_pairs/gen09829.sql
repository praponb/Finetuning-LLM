SELECT order_id, ROUND(AVG(quantity), 2) AS avg_quantity, MAX(unit_price) - MIN(unit_price) AS unit_price_range
FROM order_items
WHERE unit_price BETWEEN 130 AND 340 OR order_id IN (8, 23, 32, 38)
GROUP BY order_id
HAVING COUNT(*) < 4
ORDER BY unit_price_range;
