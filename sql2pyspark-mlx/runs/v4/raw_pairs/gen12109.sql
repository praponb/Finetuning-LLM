SELECT opened_date
FROM stores
UNION
SELECT last_restock opened_date
FROM inventory
WHERE last_restock < '2024-01-01';
