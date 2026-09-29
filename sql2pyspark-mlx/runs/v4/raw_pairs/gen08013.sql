SELECT id, store_id
FROM inventory
WHERE store_id IN (4, 5, 6)
UNION ALL
SELECT id, store_id
FROM inventory
WHERE quantity = 78
