SELECT id, quantity, store_id
FROM inventory
WHERE store_id IN (2, 3, 5)
UNION ALL
SELECT id, quantity, store_id
FROM inventory
WHERE store_id = 4
ORDER BY id;
