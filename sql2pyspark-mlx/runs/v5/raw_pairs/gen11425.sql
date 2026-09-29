SELECT *
FROM inventory
WHERE last_restock >= '2025-07-01' AND (product_id IN (1, 5, 10) OR last_restock BETWEEN '2023-08-01' AND '2025-08-01')
ORDER BY id, last_restock;
