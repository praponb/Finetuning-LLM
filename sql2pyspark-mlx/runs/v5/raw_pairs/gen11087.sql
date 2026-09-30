SELECT product_id, SUM(quantity) total_quantity
FROM order_items
WHERE quantity < 7 OR order_id IN (22, 28, 29, 36)
GROUP BY product_id
HAVING SUM(quantity) > 45
ORDER BY product_id DESC
