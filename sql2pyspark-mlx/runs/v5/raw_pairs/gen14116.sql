SELECT id, last_restock, quantity
FROM inventory
WHERE YEAR(last_restock) = 2023 AND MONTH(last_restock) IN (6, 8, 12)
