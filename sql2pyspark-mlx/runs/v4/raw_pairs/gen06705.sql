SELECT last_restock
FROM inventory
WHERE YEAR(last_restock) = 2024
UNION
SELECT last_restock
FROM inventory
WHERE store_id IN (3, 5, 6)
