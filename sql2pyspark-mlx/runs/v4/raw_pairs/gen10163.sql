SELECT MIN(last_restock) AS min_last_restock, SUM(quantity) AS total_quantity
FROM inventory
WHERE (last_restock BETWEEN '2023-04-01' AND '2024-04-04' AND store_id = 5) OR last_restock > '2023-05-01'
ORDER BY min_last_restock, total_quantity;
