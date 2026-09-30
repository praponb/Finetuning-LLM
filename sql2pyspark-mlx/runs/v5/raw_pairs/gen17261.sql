SELECT id, store_id, quantity
FROM inventory
WHERE store_id IN (1, 3, 4, 5)
UNION ALL
SELECT id, store_id, quantity
FROM inventory
WHERE YEAR(last_restock) = 2025;
