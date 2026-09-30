SELECT id, last_restock, store_id
FROM inventory
WHERE quantity <= 92
UNION
SELECT id, last_restock, store_id
FROM inventory
WHERE quantity = 215;
