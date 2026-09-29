SELECT MIN(quantity) min_quantity, MAX(last_restock) last_last_restock, SUM(quantity) total_quantity
FROM inventory
WHERE store_id IN (3, 6) AND store_id IN (1, 3, 5)
ORDER BY total_quantity DESC
