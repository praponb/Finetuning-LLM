SELECT last_restock
FROM inventory
WHERE quantity <= 175
EXCEPT
SELECT last_restock
FROM inventory
WHERE store_id IN (3, 6)
ORDER BY last_restock;
