SELECT id, store_id
FROM inventory
WHERE YEAR(last_restock) = 2025
INTERSECT
SELECT id, store_id
FROM inventory
WHERE last_restock < '2024-11-26'
ORDER BY store_id DESC
