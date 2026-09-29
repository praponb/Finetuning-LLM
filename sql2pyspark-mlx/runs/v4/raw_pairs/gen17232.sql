SELECT quantity
FROM inventory
WHERE quantity <= 126
UNION ALL
SELECT quantity
FROM inventory
WHERE store_id = 3
