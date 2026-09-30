SELECT id, store_id, ROUND(quantity * 0.85, 2) AS adjusted_quantity
FROM inventory
WHERE last_restock >= '2023-11-01';
