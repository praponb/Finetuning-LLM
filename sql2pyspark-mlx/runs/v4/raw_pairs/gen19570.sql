SELECT ss.product_id, SUM(i.quantity) AS total_quantity, MIN(i.last_restock) AS first_last_restock
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.store_id IN (4, 6)
GROUP BY ss.product_id;
