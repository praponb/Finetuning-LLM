SELECT ss.product_id, MAX(i.quantity) - MIN(i.quantity) AS quantity_range
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.last_restock > '2023-03-01'
GROUP BY ss.product_id;
