SELECT id, store_id, last_restock
FROM inventory
WHERE quantity <= 105
UNION ALL
SELECT id, store_id, last_restock
FROM inventory
WHERE last_restock > '2023-05-28'
ORDER BY store_id;
