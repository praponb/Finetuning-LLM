SELECT ss.product_id, SUM(i.quantity) AS total_quantity, AVG(i.quantity) AS avg_quantity
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.store_id IN (1, 6)
GROUP BY ss.product_id
