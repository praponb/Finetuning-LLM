SELECT ss.store_id, MAX(i.quantity) - MIN(i.quantity) AS quantity_range, COUNT(i.quantity) AS quantity_count
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY ss.store_id
