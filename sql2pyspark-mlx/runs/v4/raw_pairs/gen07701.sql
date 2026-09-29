SELECT MIN(quantity) AS min_quantity, MAX(quantity) AS max_quantity, AVG(quantity) AS avg_quantity
FROM order_items
WHERE quantity >= 4 AND (quantity IS NOT NULL OR unit_price > 260)
ORDER BY max_quantity;
