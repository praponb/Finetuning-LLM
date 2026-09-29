SELECT product_id, MAX(quantity) - MIN(quantity) AS quantity_range
FROM order_items
WHERE order_id IN (5, 18, 24, 32) AND order_id IN (6, 8, 32)
GROUP BY 1
HAVING MAX(quantity) - MIN(quantity) > 76
