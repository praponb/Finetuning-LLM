SELECT ss.store_id, COUNT(*) AS num_inventory
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.last_restock > '2024-12-01'
GROUP BY ss.store_id
HAVING COUNT(*) > 2
ORDER BY num_inventory;
