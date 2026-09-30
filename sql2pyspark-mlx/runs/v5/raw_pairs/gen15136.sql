SELECT last_restock
FROM inventory
WHERE YEAR(last_restock) = 2024
INTERSECT
SELECT last_restock
FROM inventory
WHERE last_restock < '2025-01-01';
